#!/home/pablo/Spymovil/python/pyenv/ml/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 09:19:01 2019

@author: pablo
"""

import logging
import logging.handlers
import ast
from datetime import datetime
from FUNCAUX.config import Config

# Variable global que indica donde logueo.
# La configuro al inciar los programas

#syslogmode = 'SYSLOG'
syslogmode = 'XPROCESS'

def config_logger( modo='SYSLOG'):
    # logging.basicConfig(filename='log1.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s', level = logging.DEBUG, datefmt = '%d/%m/%Y %H:%M:%S' )

    global syslogmode
    syslogmode = modo
    #print('SYSLOGMODE={}'.format(syslogmode))

    logging.basicConfig(level=logging.DEBUG)

    # formatter = logging.Formatter('SPX %(asctime)s  [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %T')
    # formatter = logging.Formatter('SPX [%(levelname)s] [%(name)s] %(message)s')
    formatter = logging.Formatter('PLC [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.handlers.SysLogHandler('/dev/log')
    handler.setFormatter(formatter)
    # Creo un logger derivado del root para usarlo en los modulos
    logger1 = logging.getLogger()
    # Le leo el handler de consola para deshabilitarlo
    lhStdout = logger1.handlers[0]  # stdout is the only handler initially
    logger1.removeHandler(lhStdout)
    # y le agrego el handler del syslog.
    logger1.addHandler(handler)
    # Creo ahora un logger child local.
    LOG = logging.getLogger('spy')
    LOG.addHandler(handler)


def log(module,function,dlgid='00000',level='INFO', msg=''):
    '''
    Se encarga de mandar la logfile el mensaje.
    Si el level es SELECT, dependiendo del dlgid se muestra o no
    Si console es ON se hace un print del mensaje
    El flush al final de print es necesario para acelerar. !!!
    '''
    debug_dlgid = Config['DEBUG']['debug_dlg']
    debug_level = Config['DEBUG']['debug_level']
    dlevel = {'INFO':0, 'WARN':1, 'ALERT':2, 'ERROR':3, 'SELECT':4, 'DEBUG':5 }

    debug_configurado = dlevel[ Config['DEBUG']['debug_level'] ]
    debug_solicitado = dlevel[level]

    #if syslogmode != 'SYSLOG':
    #    print('SPY.py TEST[level={0}, debug_level={1}, dlgid={2}, debug_dlgid={3}'.format(level, debug_level, dlgid, debug_dlgid))

    # Si el nivel que trae es mayor de lo que debo loguar, salgo
    if debug_solicitado > debug_configurado:
        return

    # Los mensajes SELECT los logueo solo para los DLGID que estan en la lista de CONFIGURACION
    if  level == 'SELECT':
        if dlgid == debug_dlgid :
            if syslogmode == 'SYSLOG':
                logging.info('SPY.py [{0}][{1}][{2}]:[{3}]'.format( dlgid,module,function,msg))
            else:
                print('{0} {1}:: [{2}][{3}][{4}]:[{5}]'.format( datetime.now(), syslogmode, dlgid, module, function, msg), flush=True)
        return

    else:
        # El resto de los mensajes los logueo TODOS ( WARN,ALERT,ERROR etc)
        if syslogmode == 'SYSLOG':
            logging.info('SPY.py [{0}][{1}][{2}]:[{3}]'.format( dlgid,module,function,msg))
        else:
            print('{0} {1}:: [{2}][{3}][{4}]:[{5}]'.format( datetime.now(), syslogmode, dlgid, module, function, msg), flush=True)

    return


if __name__ == '__main__':
    #from spy import Config
    #list_dlg = ast.literal_eval(Config['SELECT']['list_dlg'])
    pass
