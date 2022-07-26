#!/usr/bin/python3 -u
#!/opt/anaconda3/envs/mlearn/bin/python3

from multiprocessing import Process, Pool, Lock
import os
import time
import pickle
import signal
from FUNCAUX.bd_redis import BD_REDIS
from FUNCAUX.bd_gda import BD_GDA
from FUNCAUX.log import config_logger, log
from FUNCAUX import stats
from FUNCAUX.config import Config
from FUNCAUX.mbus_write import mbusWrite

DATABOUNDLESIZE=50


def procesar_reenvios(d_params):
    '''
    Lee si el dlgid tiene asociados otros para reenviarle los datos medidos.
    d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
    '''
    # Intento leer el diccionario con los datos de los reenvios de redis primero
    rh=d_params['REDIS']
    gda=d_params['GDA']
    dlgid=d_params['DLGID']
    d_data = d_params['DATOS']

    # Leo la informacion de los reenvios.
    d_reenvios = rh.get_d_reenvios(dlgid)
    if d_reenvios is None:
        # No esta en Redis: intento leerlo de GDA y actualizar REDIS
        d_reenvios_gda = gda.get_d_reenvios(dlgid)
        if d_reenvios_gda is None:
            # No hay reenvios: salgo.
            return

        # Hay datos de reenvios en GDA: actualizo redis para la proxima vez
        rh.set_d_reenvios(dlgid, d_reenvios_gda)
        d_reenvios = d_reenvios_gda.copy()

    log(module=__name__, function='process_test', level='SELECT', dlgid=dlgid, msg='D_REENVIOS={0}'.format(d_reenvios))
    log(module=__name__, function='process_test', level='SELECT', dlgid=dlgid, msg='D_DATA={0}'.format(d_data))

    l_cmds_modbus = []
    for k_dlgid in d_reenvios:
        for magname in d_reenvios[k_dlgid]:
            if magname in d_data:
                magval = float(d_data[magname])
                regaddr = d_reenvios[k_dlgid][magname]['MBUS_REGADDR']
                tipo = d_reenvios[k_dlgid][magname]['TIPO']
                # Correcciones
                tipo = ('float' if tipo == 'FLOAT' else 'integer')
                if tipo == 'integer':
                    magval = int(magval)
                #d_reenvios[dlgid][magname]['VALOR'] = magval
                l_cmds_modbus.append( (k_dlgid,regaddr, tipo, magval),)
    # l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)]
    log(module=__name__, function='procesar_reenvios', level='INFO', dlgid=dlgid, msg='L_CMDS_MODBUS({0})={1}'.format(dlgid, l_cmds_modbus))

    '''
    En l_cmds_modbus = [('PABLO1', '1965', 'float', 8.718), ('PABLO2', '1966', 'integer', 17)] tengo las tuplas que
    debo mandar a la funcion mbusWrite(self.dlgid)
    '''
    '''
    if len(l_cmds_modbus) == 0:
        # El equipo no tiene remotos asociados
        mbusWrite(dlgid)
    else:
        # Hay remotos asociados
        for t in l_cmds_modbus:
            # Chequeo que los remotos tengan una entrada en la redis. mbusWrite no lo hace.
            (t_dlgid, register, dataType, value) = t
            if not rh.exist_or_create_entry(t_dlgid):
                continue
            mbusWrite(t_dlgid, register, dataType, value)
    '''
    #
    return


def process_test():
    '''
    Recibe un boundle (array) de lineas pickled de un diccionario de los datos recibidos de un plc
    d['RCVD']
    d['ID']
    d['VER']
    d['varName']
    '''
    pid = os.getpid()
    log(module=__name__, function='process_test', level='INFO', msg='process_child START. pid={0}'.format(pid))

    gda = BD_GDA()
    rh = BD_REDIS()
    while True:
        qsize = rh.read_lqueue_length('LQ_PLCDATA')
        if qsize > 0:
            # Hay datos para procesar: arranco un worker.
            start = time.perf_counter()
            stats.init()
            log(module=__name__, function='process_test', level='INFO', msg='Redis Queue size={0}'.format(qsize))
            boundle = rh.lpop_lqueue('LQ_PLCDATA', DATABOUNDLESIZE)
            for pkline in boundle:
                d=pickle.loads(pkline)
                dlgid = d.get('ID', 'SPY000')

                # Inserto en todas las tablas
                gda.insert_dlg_raw( dlgid, d)
                gda.insert_dlg_data( dlgid, d )
                gda.insert_spx_datos( dlgid, d)
                gda.insert_spx_datos_online( dlgid, d)

                # Automatismos
                # Guardo la linea recibida (d['RCVD']) en Redis en el campo 'LINE', para otros procesamientos
                rcvd_line = d.get('RCVD',"ERROR Line")
                rh.save_line(dlgid, rcvd_line)
                #
                # Broadcasting / Reenvio de datos
                d_params={'GDA':gda,'REDIS':rh,'DLGID':dlgid,'DATOS':d }
                procesar_reenvios(d_params)

            # Fin del procesamiento del bundle
            elapsed = time.perf_counter() - start
            log(module=__name__, function='process_test', level='INFO', msg='process_child END. pid={0}, elapsed={1}'.format(pid, elapsed))
            stats.end()
        #
        else:
            log(module=__name__, function='process_test', level='INFO', msg='No hay datos en qRedis. Espero 5s....')
            time.sleep(5)
    #
    return


def clt_C_handler(signum, frame):
    exit(0)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    # Arranco el proceso que maneja los inits
    config_logger('TEST-XPROCESS')

    log(module=__name__, function='xprocess', level='INFO', msg='Debug DLGID={0}'.format(Config['DEBUG']['debug_dlg']))
    log(module=__name__, function='xprocess', level='INFO', msg='Debug LEVEL={0}'.format(Config['DEBUG']['debug_level']))

    process_test()
    while True:
        time.sleep(60)


