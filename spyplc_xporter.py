#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Este script implementa el exporter de las metricas del servidor SPY.py
El script funciona invocado por el APACHE y luego de procesar un frame, desaparece por lo tanto debemos
encontrar una forma de guardar las estadisticas en forma persistente.
Elejimos guardarlas en REDIS.
Por otro lado, ya tenemos el servidor desarrollado lo que complejiza el manejo de las variables necesarias
para generar las estadísticas ya que el procesamiento comienza en un modulo (spyplc.py) y termina en otro.
La idea es usar variables globales entre los modulos.
https://stackoverflow.com/questions/13034496/using-global-variables-between-files

Defino un archivo de variables globales ( un diccionario ) que lo voy completando por todos los modulos que lo requieran

Las metricas que vamos a implementar inicialmente son:
- Frames atendidos por minuto:
    - Frames de INITS (GAUGE)
    - Frames de DATOS (GAUGE)

- Duracion de procesamiento de frames:
    - min (GAUGE)
    - max (GAUGE)
    - avg (GAUGE)

- Lecturas en GDA (GAUGE)
- Lecturas en REDIS (GAUGE)
- Uso de la cola de mensajes de DATOS (GAUGE)
- Uso de la cola de mensajes de INIT (GAUGE)

El servidor SPY.py va a generar las metricas y las va a almacenar en REDIS de donde las leera el exporter.
La implementacion adicional que debemos agregar en SPY.py es:
- Frames de INITS: Incrementar un contador en EXPORTER_REDIS
- Frames de DATOS: Incrementar un contador en EXPORTER_REDIS
- Tiempo de procesamiento de INIT: Agrego el valor a una lista en EXPORTER_REDIS
- Lecturas de GDA en INITS: Incrementar un contador en EXPORTER_REDIS
- Lecturas de REDIS en INITS: Incrementar un contador en EXPORTER_REDIS

El exporter una vez por minuto genera las nuevas estadísticas y las deja en otros registros GAUGE que son los
que se consultan cuando se exponen los datos.

Implementacion de la generacion de stats en SPY.py:
1- En spyplc.py, al inicio se actualiza la variable 'start' con el tiempo de inicio.
2- En spy_raw_frame.py que es donde se determina el tipo de frames, se actualizan las variables:
'count_frame', y dependiendo del tipo de frame 'count_frame_ctl', 'count_frame_init', 'count_frame_data',
'count_frame_error'.
3- Al terminar, enviando una respuesta ( u_send_response ) se actualiza la variable 'end' con el tiempo de fin para
poder calcular el tiempo de procesamiento.
Tambien se actualiza el registro en redis.
Eventualmente se loguea una linea con los datos de las estadisticas de modo que el exporter pueda tomar los datos
del logfile o la redis.
4- REDIS: Guardamos la cantidad de accessos totales y la de accesos solo para leer configuracion.
Cada acceso pasa por ver si esta conectada 'connect()' y es ahi donde contamos los accesos totales.
Con GDA hago lo mismo.
Con BDSPY idem.

Cada frame genera un registro d_statistics[].
Como primer alternativa vamos serializarlo y guardarlo en una lista en REDIS ( llamada LQUEUE_STATS ) para que luego
el exporter la lea y la procese ( y borre )
El exporter nos va a dar el tiempo de procesamiento de esta estructura asi vemos si es suficiente.

    d_statistics['count_frame'] = 0
    d_statistics['count_frame_init'] = 0
    d_statistics['count_frame_data'] = 0
    d_statistics['count_frame_ctl'] = 0
    d_statistics['count_frame_error'] = 0

    d_statistics['count_accesos_GDA'] = 0
    d_statistics['count_accesos_REDIS'] = 0
    d_statistics['count_accesos_BDSPY'] = 0

    d_statistics['duracion_frame'] = 0
    d_statistics['duracion_frame_init'] = 0
    d_statistics['duracion_frame_data'] = 0
    d_statistics['duracion_frame_ctl'] = 0

    d_statistics['size_lqueue_credenciales'] = 0
    d_statistics['size_lqueue_inits'] = 0
    d_statistics['size_lqueue_data'] = 0
'''

import redis
import time
import prometheus_client as prom
from spyplc_config import Config
import pickle
import numpy as np


class SPYSTATS():
    def __init__(self):
        # TAMANO DE LAS QUEUES
        self.metrics_queues_sizes = { 'INITS':
                                 {  'l_tmp':[],
                                    'metric': prom.Gauge('size_lqueue_inits', 'Promedio de tamaño de la cola de INITS')
                                    },
                             'DATA':
                                 {  'l_tmp': [],
                                    'metric': prom.Gauge('size_lqueue_data', 'Promedio de tamaño de la cola de DATA')
                                    },
                             'CREDENCIALES':
                                 {  'l_tmp': [],
                                    'metric': prom.Gauge('size_lqueue_credenciales', 'Promedio de tamaño de la cola de CREDENCIALES')
                                  },
                             }

        # DURACION PROMEDIO DE FRAMES
        self.metrics_duracion_avg_frames = { 'ALL': {
                                                'l_tmp':[],
                                                'metric': prom.Gauge("duracion_frames_all", "Duracion promedio de frames generales")
                                                },

                                            'INITS': {
                                                'l_tmp':[],
                                                'metric': prom.Gauge("duracion_frame_inits", "Duracion promedio de frames de INITS")
                                                },

                                            'DATA': {
                                                'l_tmp': [],
                                                'metric': prom.Gauge("duracion_frame_data", "Duracion promedio de frames de DATA")
                                                },

                                            'CTL': {
                                                'l_tmp': [],
                                                'metric': prom.Gauge("duracion_frame_ctl", "Duracion promedio de frames de CTL")
                                            },
                                        }

        # ACCESOS A LAS BD
        self.metrics_bd_accesos_xmin = { 'GDA': {
                                        'l_tmp':[],
                                        'metric': prom.Gauge('accesos_GDAxmin', 'Promedio de accesos totales a BD GDA por minuto')
                                        },
                                    'REDIS': {
                                        'l_tmp': [],
                                        'metric': prom.Gauge('accesos_REDISxmin', 'Promedio de accesos totales a BD REDIS por minuto')
                                        },
                                    'BDSPY': {
                                        'l_tmp': [],
                                        'metric': prom.Gauge('accesos_BDSPYxmin', 'Promedio de accesos totales a BD BDSPY por minuto')
                                    },
                                }
        return

    def metrics_queues_sizes_init(self):
        for key in self.metrics_queues_sizes:
            self.metrics_queues_sizes[key]['l_tmp'] = []
        return

    def metrics_queues_sizes_append(self, qName, value):
        self.metrics_queues_sizes[qName]['l_tmp'].append(value)
        return

    def metrics_queues_sizes_do_calculate(self):
        '''
        Me interesa el maximo tamaño de la cola
        '''
        for key in self.metrics_queues_sizes:
            nl = np.array(self.metrics_queues_sizes[key]['l_tmp'])
            qsize = np.max( nl )
            self.metrics_queues_sizes[key]['metric'].set( qsize )
            #print('QUEUES_SIZE: {0} <{1}> {2}'.format(key, qsize_avg, self.metrics_queues_sizes[key]['l_tmp']))
            print('QUEUES_SIZE: {0} <{1}>'.format(key, qsize ))
        return

    def metrics_duracion_avg_frames_init(self):
        for key in self.metrics_duracion_avg_frames:
            self.metrics_duracion_avg_frames[key]['l_tmp'] = []
        return

    def metrics_duracion_avg_frames_append(self, qName, value):
        self.metrics_duracion_avg_frames[qName]['l_tmp'].append(value)
        return

    def metrics_duracion_avg_frames_do_calculate(self):
        '''
        La duracion la damos en milisegundos
        '''
        for key in self.metrics_duracion_avg_frames:
            nl = np.array(self.metrics_duracion_avg_frames[key]['l_tmp'])
            duracion_avg = 0
            if len(nl[nl>0]) > 0:
                duracion_avg = np.mean( nl[nl>0] )*1000
            self.metrics_duracion_avg_frames[key]['metric'].set( duracion_avg )
            #print('DURACION: {0} <{1}> {2}'.format(key, duracion_avg, self.metrics_duracion_avg_frames[key]['l_tmp']))
            print('DURACION: {0} <{1}>'.format(key, duracion_avg ))
        return

    def metrics_bd_accesos_xmin_init(self):
        for key in self.metrics_bd_accesos_xmin:
            self.metrics_bd_accesos_xmin[key]['l_tmp'] = []
        return

    def metrics_bd_accesos_xmin_append(self, qName, value):
        self.metrics_bd_accesos_xmin[qName]['l_tmp'].append(value)
        return

    def metrics_bd_accesos_xmin_do_calculate(self):
        '''
        Los accesos son la suma de todos los accesso en 15secs, proyectados a 60secs
        '''
        for key in self.metrics_bd_accesos_xmin:
            nl = np.array(self.metrics_bd_accesos_xmin[key]['l_tmp'])
            accesos_xmin = np.sum( nl[nl>0] ) * 60 / 15
            self.metrics_bd_accesos_xmin[key]['metric'].set( accesos_xmin )
            #print('ACCESOXMIN: {0} <{1}> {2}'.format(key, accesos_xmin, self.metrics_bd_accesos_xmin[key]['l_tmp']))
            print('ACCESOXMIN: {0} <{1}>'.format(key, accesos_xmin))
        return

    # -----------------------------------------------------------------------------------------------------------

    def process(self, pklist):
        # Inicializo las metricas
        self.metrics_queues_sizes_init()
        self.metrics_duracion_avg_frames_init()
        self.metrics_bd_accesos_xmin_init()
        #
        # Agrego los datos
        for pki in pklist:
            d = pickle.loads(pki)
            #print(d)
            #print("-----------------------------------------")
            self.metrics_duracion_avg_frames_append('ALL', d['duracion_frame'])
            self.metrics_duracion_avg_frames_append('INITS', d['duracion_frame_init'])
            self.metrics_duracion_avg_frames_append('DATA', d['duracion_frame_data'])
            self.metrics_duracion_avg_frames_append( 'CTL', d['duracion_frame_ctl'])

            self.metrics_bd_accesos_xmin_append('GDA', d['count_accesos_GDA'])
            self.metrics_bd_accesos_xmin_append('REDIS', d['count_accesos_REDIS'])
            self.metrics_bd_accesos_xmin_append('BDSPY', d['count_accesos_BDSPY'])

            self.metrics_queues_sizes_append('INITS', d['size_lqueue_inits'])
            self.metrics_queues_sizes_append('DATA', d['size_lqueue_data'])
            self.metrics_queues_sizes_append('CREDENCIALES', d['size_lqueue_credenciales'])
        #
        # Hago los calculos
        self.metrics_queues_sizes_do_calculate()
        self.metrics_duracion_avg_frames_do_calculate()
        self.metrics_bd_accesos_xmin_do_calculate()
        print("-----------------------------------------")
        return

    # -----------------------------------------------------------------------------------------------------------


if __name__ == '__main__':

    spy_stats = SPYSTATS()

    try:
        rh = redis.Redis(host=Config['REDIS']['host'], port=Config['REDIS']['port'], db=Config['REDIS']['db'])
    except Exception as err_var:
        print('No puedo conectarme a REDIR. ABORT !!.')
        exit(1)

    # Arranco el servidor en el puerto donde quiero publicar los datos a prometheus
    prom.start_http_server(8022)

    print('Xporter running on port 8022...')
    while True:
        # Pass1:
        # Leo todos los datos la cola REDIS de STATS que quedan en un array.
        # Cada elemento es un pickle de un dict.
        queue_length = rh.llen('LQUEUE_STATS')
        if queue_length > 0:
            p = rh.pipeline()
            p.multi()
            p.lrange('LQUEUE_STATS', 0, - 1)                # Leo toda la cola hasta ahora
            p.ltrim('LQUEUE_STATS', queue_length, -1)       # Borro los elementos leidos
            res = p.execute()
            """
            En res esta el primer elemento que son las respuestas del primer comando y el segundo elemento
            son las respuestas del segundo comando.
            El primer comando responde con una lista o sea que los resultados de la lectura de la cola
            estan en res[0]
            """
            print('Queue length: {}'.format(queue_length))
            spy_stats.process(res[0])

        time.sleep(15)