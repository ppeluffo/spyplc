#!/usr/bin/python3 -u
#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Version 2.0 @ 2022-08-02:
Ulises modifica para que se haga una insercion sola con todos los datos.

Version 1.0:
Servidor de procesamiento de frames recibidos de los PLC que están en la REDIS.
El __main__ invoca a un proceso master.
Este master crea un pool de processo child que quedan leyendo la REDIS.
Si hay datos la sacan y procesan.
'''
import random
from multiprocessing import Process, Pool, Lock, log_to_stderr, active_children
import os
import time
import pickle
import signal
import logging
import datetime as dt

from FUNCAUX.bd_redis import BD_REDIS
from FUNCAUX.bd_gda import BD_GDA
from FUNCAUX.log import config_logger, log
from FUNCAUX import stats
from FUNCAUX.config import Config
from FUNCAUX.utils import mbusWrite

MAXPOOLSIZE=5
DATABOUNDLESIZE=50


def process_child_plc_test():
    start = time.perf_counter()
    pid = os.getpid()
    #logger.info('process_child_plc START. pid={0}'.format(pid))
    #with lock:
    log(module=__name__, function='process_child_plc', level='INFO', msg='process_child_plc START. pid={0}'.format(pid))

    time.sleep(random.randint(1,10))
    elapsed = time.perf_counter() - start
    #logger.info('process_child_plc END. pid={0}, elapsed={1}'.format(pid,elapsed))
    #with lock:
    log(module=__name__, function='process_child_plc', level='INFO', msg='process_child_plc END. pid={0}, elapsed={1}'.format(pid,elapsed))
    exit (0)


def procesar_reenvios(d_params):
    '''
    Lee si el dlgid tiene asociados otros para reenviarle los datos medidos.
    d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
    '''
    # Intento leer el diccionario con los datos de los reenvios de redis primero
    rh=d_params['REDIS']
    gda=d_params['GDA']
    dlgid=d_params['DLGID']
    d_data = d_params['DATOS']

    # Leo la informacion de los reenvios.
    d_reenvios = rh.get_d_reenvios(dlgid)
    if d_reenvios is None:
        # No esta en Redis: intento leerlo de GDA y actualizar REDIS
        d_reenvios_gda = gda.get_d_reenvios(dlgid)
        if d_reenvios_gda is None:
            # No hay reenvios: salgo.
            return

        # Hay datos de reenvios en GDA: actualizo redis para la proxima vez
        rh.set_d_reenvios(dlgid, d_reenvios_gda)
        d_reenvios = d_reenvios_gda.copy()

    log(module=__name__, function='process_test', level='SELECT', dlgid=dlgid, msg='D_REENVIOS={0}'.format(d_reenvios))
    log(module=__name__, function='process_test', level='SELECT', dlgid=dlgid, msg='D_DATA={0}'.format(d_data))

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
                l_cmds_modbus.append( (k_dlgid,regaddr, tipo, magval),)
    # l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)]
    log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='L_CMDS_MODBUS({0})={1}'.format(dlgid, l_cmds_modbus))

    '''
    En l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)] tengo las tuplas que
    debo mandar a la funcion mbusWrite(self.dlgid)
    '''
    '''
    if len(l_cmds_modbus) == 0:
        # El equipo no tiene remotos asociados
        mbusWrite(dlgid)
    else:
        # Hay remotos asociados
        for t in l_cmds_modbus:
            # Chequeo que los remotos tengan una entrada en la redis. mbusWrite no lo hace.
            (t_dlgid, register, dataType, value) = t
            if not rh.exist_or_create_entry(t_dlgid):
                continue
            mbusWrite(t_dlgid, register, dataType, value)
    '''
    #
    return


def process_child_plc():
    '''
    Lee un bundle de lineas de redis.
    Si esta vacia, espera
    '''
    pid = os.getpid()
    log(module=__name__, function='process_test_plc', level='INFO', msg='process_child_plc START. pid={0}'.format(pid))

    gda = BD_GDA()
    rh = BD_REDIS()

    while True:

        boundle = rh.lpop_lqueue('LQ_PLCDATA', DATABOUNDLESIZE)
        if boundle is not None:
            qsize = len(boundle)
            if qsize > 0:
                # Hay datos para procesar: arranco un worker.
                start = time.perf_counter()
                stats.init()
                log(module=__name__, function='process_test_plc', level='INFO', msg='({0}) process_child_plc: RQsize={1}'.format(pid,qsize))

                # Version 2.0 Start.
                data = []
                for pkline in boundle:
                    d=pickle.loads(pkline)
                    dlgid = d.get('ID', 'SPY000')

                    log(module=__name__, function='process_test', level='INFO', msg='pid={0},L={1}'.format(pid,pkline))
                    
                    data.append({
                        'dlgid': dlgid,
                        'data': d
                    })
 
                    # Automatismos
                    # La linea se guarda en redis ( LINE ) en spyplc.py
                    #
                    # Broadcasting / Reenvio de datos
                    d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
                    procesar_reenvios(d_params)

                
                # Inserto en todas las tablas
                # Inserto todo en una sola consulta
                allConfigs = gda.read_dlg_insert_data(data) 
                gda.insert_dlg_raw( data )
                gda.insert_dlg_data( data )
                gda.insert_spx_datos( data, allConfigs )
                gda.insert_spx_datos_online( data, allConfigs )

                # Version 2.0: End.

                # Fin del procesamiento del bundle
                elapsed = time.perf_counter() - start
                log(module=__name__, function='process_test_plc', level='INFO', msg='({0}) process_child_plc: round elapsed={1}'.format(pid, elapsed))
                stats.end()
        #
        else:
            #log(module=__name__, function='process_test', level='INFO', msg='No hay datos en qRedis. Espero 5s....')
            time.sleep(5)

    #
    return


def process_master_plc():
    '''
    https://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes
    https://bentyeh.github.io/blog/20190722_Python-multiprocessing-progress.html
    '''
    log(module=__name__, function='process_master_plc', level='INFO', msg='process_master_plc START')
    plist = []
    #logger.info(plist)
    # Creo todos los procesos child.
    while len(plist) < MAXPOOLSIZE:
        p = Process(target=process_child_plc)
        p.start()
        plist.append(p)
    '''
    Quedo monitoreando los procesos: si alguno termina ( por errores cae ), levanto uno que lo reemplaza
    '''
    while True:
        # Saco de la plist todos los procesos child que no este vivos
        for i, p in enumerate(plist):
            if not p.is_alive():
                plist.pop(i)
                log(module=__name__, function='process_master_plc', level='INFO', msg='process_master_plc: Proceso {0} not alive;removed. L={1}'.format(i,len(plist)))

        log(module=__name__, function='process_master_plc', level='INFO', msg='process_master_plc: plistLength={0}'.format(len(plist)))

        # Vuelvo a rellenar la lista de modo de tener el maximo de procesos corriendo siempre
        while len(plist) < MAXPOOLSIZE:
            log(module=__name__, function='process_master_plc', level='INFO', msg='process_master_plc: Agrego nuevo proceso')
            p = Process(target=process_child_plc)
            p.start()
            plist.append(p)

        log(module=__name__, function='process_master_plc', level='INFO', msg='process_master_plc: Sleep 5s ...')
        time.sleep(5)


def clt_C_handler(signum, frame):
    exit(0)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    #logger = log_to_stderr(logging.DEBUG)

    # Arranco el proceso que maneja los inits
    config_logger('XPROCESS')
    log(module=__name__, function='__init__', level='ALERT', msg='XPROCESS START')
    log(module=__name__, function='__init__', level='ALERT',
        msg='XPROCESS: Redis server={0}'.format(Config['REDIS']['host']))
    log(module=__name__, function='__init__', level='ALERT',
        msg='XPROCESS: GDA url={0}'.format(Config['BDATOS']['url_gda_spymovil']))
    p1 = Process(target=process_master_plc)
    p1.start()

    # Espero para siempre
    while True:
        time.sleep(60)
