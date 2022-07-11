#!/opt/anaconda3/envs/mlearn/bin/python3

# Falta: Comunicacion entre tareas
#        Controlador que lea las estadisticas de c/tarea y presente resultados en pantalla
#        usar clases en vez de funciones.
# https://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes


import time
import numpy as np
from multiprocessing import Lock, Pool
import argparse
import signal
import os
from spyplc_sendframes import SENDFRAMES

POOL_PROCESOS = os.cpu_count()
MAX_FRAMES_X_MIN_X_PROCESO = 200
MAX_FRAMES_X_MIN_X_TOTALES = POOL_PROCESOS * MAX_FRAMES_X_MIN_X_PROCESO
TIME_BETWEEN_FRAMES = 60 / MAX_FRAMES_X_MIN_X_PROCESO

l_dlgid = ['PABLO', 'TEST01', 'TEST02', 'TEST03', 'DUNO01', 'DUNO02', 'DUNO03','DUNO04', 'DUNO05', 'DUNO06', 'DUNO07', 'DUNO08', 'DUNO09' ]


def init(l):
    global lock
    lock = l


def child_process( i, d ):
    '''
    Envia frames al server a una tasa de 100 por minuto
    Espero un tiempo aleatorio entre 1 y 10 sec para arrancar
    '''
    np.random.seed( int( time.time()/1000))
    init_await = np.random.randint(1,10)
    '''
    Veo cuantos frames voy a mandar. Siempre mando AUTH y GLOBAL y luego elijo
    de los 17 restantes. Con esto formo un ciclo de frames que repito hasta llegar al requerido
    '''
    dlgid = l_dlgid[i]

    lock.acquire()
    print('Child process {0}'.format(os.getpid()))
    print('Starting Task {0}: {1} secs to run...'.format(i, init_await))
    print('Dlgid={0}, time_inter_frames={1}'.format(dlgid,TIME_BETWEEN_FRAMES))
    lock.release()

    time.sleep(init_await)

    sendframes = SENDFRAMES()
    sendframes.set_dlgid(d['dlgid'])
    sendframes.set_server(d['host'])
    sendframes.set_port(d['port'])
    sendframes.set_fw_ver(d['fw_ver'])
    sendframes.set_script(d['script'])
    sendframes.set_path(d['path'])
    if d['verbose'] == 0:
        sendframes.set_verbose(False)
    else:
        sendframes.set_verbose(True)

    # Genero el template del frame que voy a enviar
    sendframes.prepare_payload_template()

    # Comienzo un ciclo infinito de envio de frames. En c/frame calculo valores instantaneos diferentes
    while True:
        #lock.acquire()
        #print("{0}: sending frame...".format(os.getpid()))
        #lock.release()
        start=time.perf_counter()
        sendframes.fill_payload()
        sendframes.send()
        elapsed=time.perf_counter() - start
        time_to_sleep = TIME_BETWEEN_FRAMES - elapsed
        #print('ELAPSED={0}, time_to_sleep={1}'.format(elapsed, time_to_sleep))
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)


def process_arguments():
    '''
    Proceso la linea de comandos.
    d_vp es un diccionario donde guardamos todas las variables del programa
    Corrijo los parametros que sean necesarios
    '''

    parser = argparse.ArgumentParser(description='Prueba de carga de frames al servidor SPYPLC')
    parser.add_argument('-s', '--server', dest='host', action='store', default='127.0.0.1',
                        help='IP del servidor al cual conectarse')
    parser.add_argument('-p', '--port', dest='port', action='store', default='80',
                        help='PORT del servidor al cual conectarse')
    parser.add_argument('-d', '--dlgid', dest='dlgid', action='store', default='PABLO',
                        help='ID del datalogger a usar')
    parser.add_argument('-f', '--fw', dest='fw_ver', action='store', default='4.0.4b',
                        help='Version del firmware a usar en los frames')
    parser.add_argument('-i', '--script', dest='script', action='store', default='spyplc.py',
                        help='Nombre del script a usar')
    parser.add_argument('-t', '--path', dest='path', action='store', default='AUTOM',
                        help='Nombre del directorio donde se encuentra el script a usar')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count', default=0,
                        help='Verbose')
    parser.add_argument('-n', '--nro_frames_x_min', dest='frames_x_min', action='store', default=100,
                        help='Cantidad de frames por minuto')

    args = parser.parse_args()
    d_args = vars(args)
    return d_args


def print_banner(d):
    '''
    Muestro los parametros del proceso iniciales
    '''
    os.system('clear')
    print('Inicio load test SPXPLC R001')
    print('{0} procesos paralelos'.format(d['nro_process_to_start']))
    print('{0} frames por proceso'.format(MAX_FRAMES_X_MIN_X_PROCESO))
    print('{0} frames totales x min'.format(d['frames_x_min']))
    return


def clt_C_handler(signum, frame):
    exit(0)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    # Diccionario con variables generales para intercambiar entre funciones
    d_args = process_arguments()

    # Completo los items que faltan
    max_procesos_to_start = int(np.ceil( int(d_args['frames_x_min']) / MAX_FRAMES_X_MIN_X_PROCESO))
    d_args['nro_process_to_start'] = max_procesos_to_start
    # El numero de frames totales no puede superar al maximo.
    d_args['frames_x_min'] = max_procesos_to_start * MAX_FRAMES_X_MIN_X_PROCESO

    print_banner(d_args)

     # Creo un pool de procesos
    l = Lock()
    plist = []
    pool = Pool(initializer=init, initargs=(l,))

    for i in range (max_procesos_to_start):
        p = pool.apply_async( child_process, args=(i,d_args,))
        plist.append(p)

    # y espero para siempre !!!
    #_ = [p.get() for p in plist]  # Espero que el pool termine de procesar
    while True:
        time.sleep(60)

    exit (0)
