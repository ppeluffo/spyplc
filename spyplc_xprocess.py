#!/opt/anaconda3/envs/mlearn/bin/python3
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
'''

from multiprocessing import Process, Pool
from spyplc_bd_redis import BD_REDIS
from spyplc_bd_gda import BD_GDA
import os
from spyplc_log import config_logger, log
import time
import pickle
import spyplc_stats

def process_child(lines):
    '''
    Recibe un boundle (array) de lineas pickled de un diccionario de los datos recibidos de un plc
    d['RCVD']
    d['ID']
    d['VER']
    d['varName']
    '''
    spyplc_stats.init()
    pid = os.getpid()
    log(module=__name__, function='process_child', level='INFO', msg='XPROCESS-PLC::process_child START. pid={0}'.format(pid))
    start = time.perf_counter()
    gda = BD_GDA()
    rh = BD_REDIS()
    for pkline in lines:
        d=pickle.loads(pkline)
        dlgid = d.get('ID', 'SPY000')
        log(module=__name__, function='process_child_inits', dlgid=dlgid, level='SELECT', msg='XPROCESS-PLC pid={0}, dconf={1}'.format(pid,d))

        gda.insert_dlg_raw( dlgid, d)
        gda.insert_dlg_data( dlgid, d )
        gda.insert_spx_datos( dlgid, d)
        gda.insert_spx_datos_online( dlgid, d)

        # Automatismos
        rcvd_line = d.get('RCVD',"ERROR Line")
        rh.save_line(dlgid, rcvd_line)

    elapsed = time.perf_counter() - start
    log(module=__name__, function='process_child', level='INFO', msg='XPROCESS-PLC::process_child END. pid={0}, elapsed={1}'.format(pid, elapsed))
    spyplc_stats.end()

def process_master():
    '''
    Cada 15s lee la cantidad de datos de la cola de INITS y dispara un child por cada 100 registros.
    Hasta un maximo de 5 childs (pool)
    '''
    log(module=__name__, function='process_master', level='INFO', msg='XPROCESS-PLC::process_master START')
    pool = Pool(5)
    redis = BD_REDIS()
    if not redis.connect():
        log(module=__name__, function='process_master', level='ERROR', msg='ERROR: XPROCESS-PLC Redis Not connected. EXIT !!')
        exit(1)

    while True:
        plist = []
        max_size = redis.read_lqueue_length('LQ_PLCDATA')
        '''
        Mientras hallan datos en la cola, extraigo de a 100 como un boundle y los voy dando a 
        procesos del pool
        '''
        while redis.read_lqueue_length('LQ_PLCDATA') > 1400:
            boundle = redis.lpop_lqueue('LQ_PLCDATA', 50)
            p = pool.apply_async( process_child, args=(boundle,))
            plist.append(p)

        _ = [p.get() for p in plist]   # Espero que el pool termine de procesar
        print('Espero 5 secs')  # Solo para debug
        time.sleep(5)


if __name__ == '__main__':
    # Arranco el proceso que maneja los inits
    config_logger()
    log(module=__name__, function='__init__', level='ALERT',  msg='XPROCESS-PLC START')

    p1 = Process(target=process_master)
    p1.start()
    
    # Espero para siempre
    while True:
        time.sleep(60)

