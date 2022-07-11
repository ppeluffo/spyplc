#!/opt/anaconda3/envs/mlearn/bin/python3
'''
https://stackoverflow.com/questions/60100749/how-to-share-an-instance-of-a-class-across-an-entire-project
https://refactoring.guru/es/design-patterns/singleton/python/example#example-1
https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
https://www.geeksforgeeks.org/singleton-pattern-in-python-a-complete-guide/

Voy a implementarlo como un modulo singleton.

Las estadisticas que nos interesan son:
- Contador de cada tipo de frame procesado
- Contador de c/acceso a c/BD
- Duracion del procesamiento de c/tipo de frame
- Tamaño de las colas
'''

import time
import pickle

d_statistics = {'start':0,
                'end':0,
                'count_frame':0,
                'count_frame_data':0,
                'count_accesos_GDA':0,
                'count_accesos_REDIS':0,
                'duracion_frame':0,
                'duracion_frame_data':0,
                'size_lqueue_data':0,
                'count_errors':0
            }


def init():
    d_statistics['start'] = time.perf_counter()
    d_statistics['end'] = 0
    d_statistics['count_frame'] = 0
    d_statistics['count_frame_data'] = 0
    d_statistics['count_accesos_GDA'] = 0
    d_statistics['count_accesos_REDIS'] = 0
    d_statistics['duracion_frame'] = 0
    d_statistics['duracion_frame_data'] = 0
    d_statistics['size_lqueue_data'] = 0
    d_statistics['count_errors'] = 0


def inc_count_errors():
    d_statistics['count_errors'] += 1


def inc_count_frame():
    d_statistics['count_frame'] += 1


def inc_count_frame_data():
    d_statistics['count_frame_data'] += 1


def inc_count_accesos_GDA():
    d_statistics['count_accesos_GDA'] += 1


def inc_count_accesos_REDIS():
    d_statistics['count_accesos_REDIS'] += 1


def set_size_lqueue_data(qsize):
    d_statistics['size_lqueue_data'] = qsize


def end():
    from spyplc_bd_redis import BD_REDIS
    bdr = BD_REDIS()

    d_statistics['end'] = time.perf_counter()
    d_statistics['duracion_frame'] = d_statistics['end'] - d_statistics['start']

    # Leemos la profundidad de las colas
    set_size_lqueue_data(bdr.read_lqueue_length('LQ_PLCDATA'))

    # Calculo la duracion de los tipos de frames
    if d_statistics['count_frame_data'] > 0:
        d_statistics['duracion_frame_data'] = d_statistics['duracion_frame']

    pkl = pickle.dumps(d_statistics)
    bdr.save_statistics(pkl)

    from spyplc_log import log

    logMsg = (f"STATS: "
            f"dtime={d_statistics['duracion_frame']:.04f},"
            f"cfD={d_statistics['count_frame_data']},"
            f"aGDA={d_statistics['count_accesos_GDA']},"
            f"aRED={d_statistics['count_accesos_REDIS']},"
            f"dFD={d_statistics['duracion_frame_data']:.04f},"
            f"qD={d_statistics['size_lqueue_data']}"
        )

    log(module=__name__, function='STATS', level='ALERT', msg=logMsg )
    return





