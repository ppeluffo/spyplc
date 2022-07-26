#!/opt/anaconda3/envs/mlearn/bin/python3

'''
Script que envia en ráfaga todos los frames manejables por el SPY.py
Acepta argumentos ( ver spyplc_test_xframe.py -h )
'''

import time
import argparse
from FUNCAUX.sendframes import SENDFRAMES
from FUNCAUX.log import config_logger, log

if __name__ == '__main__':

    config_logger('XFRAME')
    start = time.time()
    parser = argparse.ArgumentParser(description='Procesamiento de frames al servidor SPYPLC')
    parser.add_argument('-s','--server', dest='host', action='store', default = '127.0.0.1',
                        help = 'IP del servidor al cual conectarse')
    parser.add_argument('-p','--port', dest='port', action='store', default = '80',
                        help = 'PORT del servidor al cual conectarse')
    parser.add_argument('-d','--dlgid', dest='dlgid', action='store', default = 'PABLO',
                        help = 'ID del datalogger a usar')
    parser.add_argument('-f','--fw', dest='fw_ver', action='store', default = '4.0.4b',
                        help = 'Version del firmware a usar en los frames')
    parser.add_argument('-i','--script', dest='script', action='store', default = 'spyplc.py',
                        help = 'Nombre del script a usar')
    parser.add_argument('-t','--path', dest='path', action='store', default = 'AUTOM',
                        help = 'Nombre del directorio donde se encuentra el script a usar')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default = 0,
                        help = 'Verbose')

    args = parser.parse_args()
    d_args = vars(args)

    print('Testing SPYPLC....')
    sendframes = SENDFRAMES()

    sendframes.set_dlgid(d_args['dlgid'])
    sendframes.set_server(d_args['host'])
    sendframes.set_port(d_args['port'])
    sendframes.set_fw_ver(d_args['fw_ver'])
    sendframes.set_script(d_args['script'])
    sendframes.set_path(d_args['path'])
    if d_args['verbose'] == 0:
        sendframes.set_verbose(False)
    else:
        sendframes.set_verbose(True)

    sendframes.prepare_payload_template()
    sendframes.send()

    # Summary:
    print('\nSUMMARY:')
    start = time.perf_counter()
    sendframes.get_params()
    elapsed = time.perf_counter() - start
    print('Total time = {0}.'.format(elapsed))
