#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u
'''
Se implementan los diferentes procesadores de colas de REDIS.
Las colas que tenemos son: LQUEUE_DATOS, LQUEUE_CREDENCIALES ,LQUEUE_INITS
Cada cola tiene el mismo foramto: c/elemento son datos serializados.
Debemos des-serializarlos y procesarlos.
El sistema principal abre inicialmente solo 3 procesos y se queda monitoreando las 3 colas.
Si se exceden los tamaños de las mismas, se comienzan a abrir nuevos procesos en paralelo.
Cuando el tamaño retorna a su valor normal, van muriendo.
Ponemos un limite a los procesos que pueden abrirse.
Generamos estadisticas que van al mismo registro en Redis y de logs para que puedan verse desde grafana.

Tenemos 3 tipos de procesos: uno para cada tipo de cola.
El principal del grupo cada 15s controla el tamaño de su cola. (LLEN queue_name)
Cada 100 datos abre un subproceso que los toma y los procesa.
El maximo es de 10 procesos del mismo tipo.
Registra en Grafana cuantos procesos abre en c/ciclo y cuantos registro habia inicialmente en la cola

1- Agrego salvar la linea en REDIS

'''
import random
from multiprocessing import Process, Pool, Lock
import os
import time
import pickle
import signal
from FUNCAUX.bd_redis import BD_REDIS
from FUNCAUX.bd_gda import BD_GDA
from FUNCAUX.log import config_logger, log
from FUNCAUX import stats
from FUNCAUX.config import Config
from FUNCAUX.utils import mbusWrite

MAXPOOLSIZE=5
DATABOUNDLESIZE=50


def procesar_reenvios(d_params, lock):
    '''
    Lee si el dlgid tiene asociados otros para reenviarle los datos medidos.
    d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }


    D_REENVIOS={'PABLO1': {'H': {'MBUS_SLAVE': '1','MBUS_REGADDR': '1962','TIPO': 'FLOAT','CODEC': 'c0123'},
                          'pA': {'MBUS_SLAVE': '1','MBUS_REGADDR': '1965','TIPO': 'FLOAT','CODEC': 'c0123'}},
                'PABLO2': {'H': {'MBUS_SLAVE': '2','MBUS_REGADDR': '1963','TIPO': 'FLOAT','CODEC': 'c3210'},
                          'pB': {'MBUS_SLAVE': '2','MBUS_REGADDR': '1966','TIPO': 'i16','CODEC': 'c3210'}},
                'PABLO3': {'H': {'MBUS_SLAVE': '3','MBUS_REGADDR': '1964','TIPO': 'FLOAT','CODEC': 'c3210'}}
                }

    D_DATA={'ID': 'PABLO','VER': '4.0.4b','q0': '44.902','pA': '8.718','pB': '17.393','T1': '64.629','T2': '49.026',
            'T3': '46.597','T4': '44.093','bt': '12.01',
            'RCVD': 'ID:PABLO;VER:4.0.4b;q0:44.902;pA:8.718;pB:17.393;T1:64.629;T2:49.026;T3:46.597;T4:44.093;bt:12.01'
            }
    l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)]

    '''
    # Intento leer el diccionario con los datos de los reenvios de redis primero
    # Parametros de entrada
    rh=d_params['REDIS']
    gda=d_params['GDA']
    dlgid=d_params['DLGID']
    d_data = d_params['DATOS']

    # Leo la informacion de los reenvios. Primero de REDIS y sino intento en GDA ( y luego cacheo )
    d_reenvios = rh.get_d_reenvios(dlgid)
    if d_reenvios is None:
        # No esta en Redis: intento leerlo de GDA y actualizar REDIS
        d_reenvios_gda = gda.get_d_reenvios(dlgid)
        if d_reenvios_gda is None:
            # No hay reenvios: salgo.
            return

        # Hay datos de reenvios en GDA: actualizo redis ( cacheo ) para la proxima vez
        rh.set_d_reenvios(dlgid, d_reenvios_gda)
        d_reenvios = d_reenvios_gda.copy()

    with lock:
        log(module=__name__, function='procesar_reenvios', level='INFO', msg='D_REENVIOS={0}'.format(d_reenvios))
        log(module=__name__, function='procesar_reenvios', level='INFO', msg='D_DATA={0}'.format(d_data))
    '''
    En d_reenvios tengo los datos de los dataloggers remotos
    En d_data tengo los datos de las medidas de este datalogger.
    Creo una lista con tuplas de los comandos a enviar a los dataloggers remotos ( REDIS )
    De c/dlg_remoto tomo el nombre de la variable y los datos de modbus.
    Del d_data tomo el valor de la medida.
    Con esto armo una tupla (dlgid,regaddr, tipo, magval) que la inserto en la lista de comandos a procesar.
    '''
    l_cmds_modbus = []
    for k_dlgid in d_reenvios:
        for magname in d_reenvios[k_dlgid]:
            if magname in d_data:
                magval = float(d_data[magname])
                regaddr = d_reenvios[k_dlgid][magname]['MBUS_REGADDR']
                tipo = d_reenvios[k_dlgid][magname]['TIPO']
                # Correcciones
                tipo = ('float' if tipo == 'FLOAT' else 'integer')
                if tipo == 'integer':
                    magval = int(magval)
                #d_reenvios[dlgid][magname]['VALOR'] = magval
                l_cmds_modbus.append( (k_dlgid, regaddr, tipo, magval),)
    # l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)]
    with lock:
        log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='L_CMDS_MODBUS({0})={1}'.format(dlgid, l_cmds_modbus))
    #
    # Inserto los datos en lo REDIS de los dlg remotos
    if len(l_cmds_modbus) == 0:
        with lock:
            log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='mbusWrite EMPTY')
        mbusWrite(dlgid)
        return

    for t in l_cmds_modbus:
        (dlgid_rem, regaddr, tipo, magval) = t
        mbusWrite(dlgid_rem, regaddr, tipo, magval)

    return


def process_child(lines):
    '''
    Recibe un boundle (array) de lineas pickled de un diccionario de los datos recibidos de un plc
    d['RCVD']
    d['ID']
    d['VER']
    d['varName']
    '''
    stats.init()
    pid = os.getpid()
    with lock:
        log(module=__name__, function='process_child', level='INFO', msg='process_child START. pid={0}'.format(pid))

    start = time.perf_counter()
    #time.sleep(random.randint(5,10))

    gda = BD_GDA()
    rh = BD_REDIS()
    for pkline in lines:
        d=pickle.loads(pkline)
        dlgid = d.get('ID', 'SPY000')
        with lock:
            log(module=__name__, function='process_child_inits', dlgid=dlgid, level='SELECT', msg='pid={0}, dconf={1}'.format(pid,d))

        # Inserto en todas las tablas
        gda.insert_dlg_raw( dlgid, d)
        gda.insert_dlg_data( dlgid, d )
        gda.insert_spx_datos( dlgid, d)
        gda.insert_spx_datos_online( dlgid, d)

        # Automatismos
        # Guardo la linea recibida (d['RCVD']) en Redis en el campo 'LINE', para otros procesamientos
        rcvd_line = d.get('RCVD',"ERROR Line")
        rh.save_line(dlgid, rcvd_line)
        #
        # Broadcasting / Reenvio de datos
        d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
        procesar_reenvios(d_params, lock)

    # Fin del procesamiento del bundle
    elapsed = time.perf_counter() - start
    with lock:
        log(module=__name__, function='process_child', level='INFO', msg='process_child END. pid={0}, elapsed={1}'.format(pid, elapsed))

    stats.end()


def process_master():
    '''
    Cada 15s lee la cantidad de datos de la cola de INITS y dispara un child por cada 100 registros.
    Hasta un maximo de 5 childs (pool)

    https://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes
    https://bentyeh.github.io/blog/20190722_Python-multiprocessing-progress.html

                                Datos en la cola REDIS
                          Hay_datos    |   No_hay_datos
          --------------------------- |------------------------
          Pool:  espacio: | arranco    |   Espero
                          | worker     |
            --------------|------------|----------------------
             SIN espacio: | Espero     |  Espero
                          | espacio    |  espacio
                          | pool       |  pool
    '''
    log(module=__name__, function='process_master', level='INFO', msg='process_master START')
    # Creo un pool de 5 workers
    # Debería asignar de a 5 y cuando vayan quedando lugares arrancar nuevos.
    # p es un async_result.
    l = Lock()
    pool = Pool(MAXPOOLSIZE, initializer=init, initargs=(l,))
    plist = []
    #
    redis = BD_REDIS()
    if not redis.connect():
        with l:
            log(module=__name__, function='process_master', level='ERROR', msg='ERROR: Redis Not connected. EXIT !!')
        exit(1)
    #
    while True:
        # ASIGNACION DE TRABAJOS
        # Si hay espacio en el pool y datos en la cola, arranco un nuevo worker.
        if len(plist) < MAXPOOLSIZE:
            # Hay espacio en el pool
            qsize = redis.read_lqueue_length('LQ_PLCDATA')
            if qsize > 0:
                # Hay datos para procesar: arranco un worker.
                boundle = redis.lpop_lqueue('LQ_PLCDATA', DATABOUNDLESIZE)
                with l:
                    log(module=__name__, function='process_master', level='INFO', msg='New pool proccess, Redis queue size={0}'.format(qsize))
                p = pool.apply_async(process_child, args=(boundle,))
                plist.append(p)
                continue
            else:
                # Hay espacio o datos en el pool pero NO hay datos para procesar: Espero
                with l:
                    log(module=__name__, function='process_master', level='WARN', msg='Pool size OK ({0}). No data in redis queue.Await 5...'.format(len(plist)))
                time.sleep(5)
                continue
            #

        # SIN ESPACIO DE POOL::MONITOREO
        # Si la lista de workers esta llena, espero que alguno termine para asignarle mas trabajo.
        with l:
            log(module=__name__, function='process_master', level='INFO', msg='Monitoreo...')
        f_exit = False
        # Espero por espacio en el pool....
        while not f_exit:
            # Cuando alguno termino, lo saco de la lista y voy al ciclo de asignar un nuevo trabajo.
            for pos, result in enumerate(plist):
                try:
                    if result.successful():
                        # Termino: lo remuevo de la lista y dejo espacio para otro.
                        plist.pop(pos)
                        with l:
                            log(module=__name__, function='process_master', level='INFO', msg='Pool room available {0} slots., pool queue size={1}'.format(pos, len(plist)))
                        f_exit = True
                except:
                    pass
            #
            if not f_exit:
                with l:
                    log(module=__name__, function='process_master', level='INFO', msg='Espero por espacio en el pool...')
                time.sleep(5)
            #


def clt_C_handler(signum, frame):
    exit(0)


def init(l):
    global lock
    lock = l


if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    # Arranco el proceso que maneja los inits
    config_logger('XPROCESS')
    log(module=__name__, function='__init__', level='ALERT',  msg='XPROCESS START')
    log(module=__name__, function='__init__', level='ALERT', msg='XPROCESS: Redis server={0}'.format(Config['REDIS']['host']))
    log(module=__name__, function='__init__', level='ALERT', msg='XPROCESS: GDA url={0}'.format(Config['BDATOS']['url_gda_spymovil']))
    p1 = Process(target=process_master)
    p1.start()
    
    # Espero para siempre
    while True:
        time.sleep(60)

