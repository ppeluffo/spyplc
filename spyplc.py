#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u
#

"""
-----------------------------------------------------------------------------------------------------
Version: 1.0.1 @ 2022-08-05
1) Excepcion 001:
Se coloca en data_frame.py para el manejo de los PLC de PAYSANDU.
2) La inserción del LINE a la redis se hace en spyplc.py y no en xprocess porque los automatismos (paysandú)
   requieren que esta la linea insertada antes de ejecutarse
3) En la redis la linea se inserta con el formato:
LINE=DATE:220802;TIME:122441;UMOD:100;MOD:102;UFREQ:100;PS:0.00;QS:38.81;HC:3.92;VF:46.05;VC:0.00;VT:0.00;ST:20562;
En spyplc_xprocess::process_child_plc, en la seccion Automatismos se invoca a  rh.save_line(dlgid, rcvd_line).

-----------------------------------------------------------------------------------------------------
Version: 1.0.0 @ 2022-07-01
Este servidor solo atiende a los PLC que transmiten directo por medio de un router LTE.
Solo hay un frame que es de datos.
El formato es:  ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

Funcionamiento:
Cuando llega un frame se loguea y se le pasa a la clase DATA_frame para que lo procese.
Esta clase hace:
    1-Elimina el ultimo caracter si no es digito ( para que no de error mas adelante en el parseo )
    2-Genera un diccionario de los nombres de variables : valor
    3-Usa la clase redis::enqueue_data_record para que serialize la linea recibida y la encole para su posterior
      procesamiento
    4-Le manda la respuesta al PLC agregando los datos que tenga la redis de este.


Testing:
- Con telnet:
telnet localhost 80
GET /cgi-bin/AUTOM/spyplc.py?ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
 HTTP/1.1
Host: www.spymovil.com

telnet www.spymovil.com 90
GET /cgi-bin/AUTOM/spyplc.py?ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11
HTTP/1.1
Host: www.spymovil.com

- Con browser:
> usamos el url: http://localhost/cgi-bin/AUTOM/spyplc.py?ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

"""

import os
import sys
from FUNCAUX.data_frame import DATA_frame
from FUNCAUX.log import *
from FUNCAUX.config import Config
from FUNCAUX import stats

version = '1.0.0 @ 2022-07-01'
# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # Lo primero es configurar el logger
    config_logger('SYSLOG')
    query_string = ''

    stats.init()

    # Modo consola ?
    if len(sys.argv) == 2:
        if sys.argv[1] == 'DEBUG_DATA':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data']
            os.environ['QUERY_STRING'] = query_string
            log(module=__name__, function='__init__', level='WARN',msg='MODO CONSOLA: query_string: {0}'.format(query_string))

    # Leo del cgi
    query_string = os.environ.get('QUERY_STRING')
    log(module=__name__, function='__init__', level='INFO', msg='QS: {0}'.format(query_string))
    if query_string is None:
        log(module=__name__, function='__init__', level='ALERT', msg='ERROR QS NULL')
        exit(1)

    # Proceso.
    pid = os.getpid()
    log(module=__name__, function='__init__', level='ALERT', msg='PID:{0} RX:[{1}]'.format( pid, query_string))
    data_frame = DATA_frame(query_string)
    dlgid, response = data_frame.process()
    log(module=__name__, function='__init__', level='ALERT', msg='PID:{0} DLGID:{1}: Process OK: RSP={2}'.format(pid, dlgid, response))
    stats.end()





