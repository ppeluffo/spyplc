#!/opt/anaconda3/envs/mlearn/bin/python3

"""
Version: 1.0.0 @ 2022-07-01
Este servidor solo atiende a los PLC que transmiten directo por medio de un router LTE.
Solo hay un frame que es de datos.
El formato es:  ID:PABLO;VER:4.0.4a;PA:3.21;PB:1.34;H:4.56;bt:10.11

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
from spyplc_data_frame import DATA_frame
from spyplc_log import *
from spyplc_config import Config
import spyplc_stats

version = '1.0.0 @ 2022-07-01'
# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # Lo primero es configurar el logger
    config_logger()
    query_string = ''

    spyplc_stats.init()

    # Modo consola ?
    if len(sys.argv) == 2:

        if sys.argv[1] == 'DEBUG_DATA':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data']
            os.environ['QUERY_STRING'] = query_string
            log(module=__name__, function='__init__', level='WARN',msg='MODO CONSOLA: query_string: {0}'.format(query_string))

    # Leo del cgi
    query_string = os.environ.get('QUERY_STRING')
    #log(module=__name__, function='__init__', level='INFO', msg='QS: {0}'.format(query_string))
    if query_string is None:
        log(module=__name__, function='__init__', level='ALERT', msg='ERROR QS NULL')
        exit(1)

    # Proceso.
    pid = os.getpid()
    log(module=__name__, function='__init__', level='ALERT', msg='{0} RX:[{1}]'.format( pid, query_string))
    data_frame = DATA_frame(query_string)
    dlgid, response = data_frame.process()
    log(module=__name__, function='__init__', level='ALERT', msg='{0} {1}: Process OK: rsp={2}'.format(pid, dlgid, response))
    spyplc_stats.end()





