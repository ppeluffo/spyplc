#!/opt/anaconda3/envs/mlearn/bin/python3

from datetime import datetime as dt
import sys
from FUNCAUX import stats
from FUNCAUX.log import log
from FUNCAUX.bd_redis import BD_REDIS
from FUNCAUX.mbus_write import mbusWrite

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

        stats.inc_count_frame()
        stats.inc_count_frame_data()

        log(module=__name__, function='__init__', level='SELECT',dlgid=self.dlgid, msg='start')
        return

    def send_response(self):
        response = '{}'.format(self.response)
        try:
            print('Content-type: text/html\n\n',end='')
            print('<html><body><h1>{0}</h1></body></html>'.format(response))
        except Exception as e:
            stats.inc_count_errors()
            log(module=__name__, function='u_send_response',level='ERROR',dlgid=self.dlgid,msg='EXCEPTION=[{0}]'.format(e))
            return

        log(module=__name__, function='send_response',level='SELECT',dlgid=self.dlgid, msg='RSP={0}'.format(response))
        return

    def convert_rcvd_line_to_old_format(self, d):
        '''
        Convierte la nueva linea recibida:
        ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
        al formato anterior:
        DATE:220802;TIME:122441;PA:3.21;PB:1.34;H:4.56;bt:10.11
        '''
        rcvd_line = d.get('RCVD', "ERROR Line")
        l = rcvd_line.split(d['VER']);
        payload = l[1]  # ';PA:3.21;PB:1.34;H:4.56;bt:10.11'
        # Agrego el time stamp
        timestamp = dt.datetime.now().strftime("DATE:%02Y%02m%02d;TIME:%02H%02M")
        rcvd_line_new_format = timestamp + payload
        return (rcvd_line_new_format)

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
            stats.inc_count_errors()
            log(module=__name__, function='process',level='ERROR',msg='DECODE ERROR: QS={0}'.format(self.query_string))

        self.dlgid = d.get('ID', '00000')
        self.version = d.get('VER', 'R0.0.0')
        log(module=__name__, function='process', level='SELECT',dlgid=self.dlgid, msg='D={0}</br>'.format(d))

        self.rh = BD_REDIS()

        # Automatismos
        # Guardo la linea recibida (d['RCVD']) en Redis en el campo 'LINE', para otros procesamientos
        # El formato debe ser igual al original.
        rcvd_line_new_format = self.convert_rcvd_line_to_old_format(d)
        rh.save_line(dlgid, rcvd_line_new_format)

        # Guardo el frame el Redis
        self.rh.enqueue_data_record(d)

        # Preparo la respuesta y transmito. Agrego las ordenes MODBUS para el PLC
        self.response += '{};'.format(datetime.now().strftime('%y%m%d%H%M'))

        # Copio de "lastMODBUS" a "MODBUS"
        #DEBUG mbusWrite(self.dlgid)
        # Agrego la linea MODBUS a la respuesta y borro.
        #DEBUG self.response += self.rh.get_modbusline(self.dlgid, clear=True)

        #-----------------------------------------------------------------------------------
        # Excepcion 001: Atencion a PLC de paysandu
        l_equipos = ['CTRLPAY01', 'CTRLPAY02', 'CTRLPAY03', 'CTRLTEST01']
        if self.dlgid in l_equipos:
            sys.path.insert(1, '/datos/cgi-bin/spx/AUTOMATISMOS')
            try:
                import serv_APP_selection
                serv_APP_selection.main(self.dlgid)
            except:
                self.response="ERROR LOAD MODULE serv_APP_selection"

        # -----------------------------------------------------------------------------------


        self.send_response()
        sys.stdout.flush()

        return self.dlgid, self.response


