#!/opt/anaconda3/envs/mlearn/bin/python3

from spyplc_log import log
from datetime import datetime
from spyplc_bd_redis import BD_REDIS
import sys
import spyplc_stats
# ------------------------------------------------------------------------------


class DATA_frame:
    # Esta clase esta especializada en los frames de datos.
    # ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

    def __init__(self, query_string):
        self.query_string = query_string        #  ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
        self.dlgid = None
        self.version = None
        self.rh = None
        self.response = ''

        spyplc_stats.inc_count_frame()
        spyplc_stats.inc_count_frame_data()

        log(module=__name__, function='__init__', level='SELECT',dlgid=self.dlgid, msg='start')
        return

    def send_response(self):
        response = '{}'.format(self.response)
        try:
            print('Content-type: text/html\n\n',end='')
            print('<html><body><h1>{0}</h1></body></html>'.format(response))
        except Exception as e:
            spyplc_stats.inc_count_errors()
            log(module=__name__, function='u_send_response',level='ERROR',dlgid=self.dlgid,msg='EXCEPTION:[{0}]'.format(e))
            return

        log(module=__name__, function='send_response',level='SELECT',dlgid=self.dlgid, msg='RSP={0}'.format(response))
        return

    def process(self):
        # Realizo todos los pasos necesarios en el payload para generar la respuesta al datalooger e insertar
        # los datos en GDA
        #log(module=__name__, function='process', level='INFO',msg='DEBUG QS={0}</br>'.format(self.query_string))
        # Ajusto formato. Elimino el ultimo caracter.
        if not self.query_string[-1].isdigit():
            self.query_string = self.query_string[:-1]
        try:
            d = {k:v for (k, v) in [x.split(':') for x in self.query_string.split(';')]}
            d['RCVD'] = self.query_string
        except:
            spyplc_stats.inc_count_errors()
            log(module=__name__, function='process',level='ERROR',msg='DECODE ERROR: QS={0}'.format(self.query_string))

        self.dlgid = d.get('ID', '00000')
        self.version = d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='SELECT',dlgid=self.dlgid, msg='DEBUG D={0}</br>'.format(d))

        self.rh = BD_REDIS()

        # Guardo el frame el Redis
        self.rh.enqueue_data_record(d)

        # Preparo la respuesta y transmito. Agrego las ordenes para el PLC
        self.response += '{};'.format(datetime.now().strftime('%y%m%d%H%M'))
        self.response += self.rh.get_bcastline(self.dlgid)

        self.send_response()
        sys.stdout.flush()

        return self.dlgid, self.response


