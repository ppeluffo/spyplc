#!/opt/anaconda3/envs/mlearn/bin/python3

'''
La configuracion en REDIS va en un HASH con la estructura:
'PABLO':{ 'RESET':True,'PSLOT':-1,'DATASOURCE':'ose','LINE':'....','MODBUS':NULL }
'324572109652':{ 'DLGID':'PABLO'}

Los inits ( Frames AUTH ) van a quedar en una lista: L_INITS
Los frames de datos quedan en otra lista L_DATOS
'''

from spyplc_config import Config
from spyplc_log import log
import redis
import pickle
import spyplc_stats


class BD_REDIS:

    def __init__(self):
        self.connected = False
        self.handle = None

    def connect(self):
        '''
        Cada acceso primero verifica si esta conectada asi que aqui incrementamos el contador de accesos.
        '''
        if self.connected:
            spyplc_stats.inc_count_accesos_REDIS()
            return True

        try:
            self.handle = redis.Redis(host=Config['REDIS']['host'], port=Config['REDIS']['port'], db=Config['REDIS']['db'])
            self.connected = True
            spyplc_stats.inc_count_accesos_REDIS()
        except Exception as err_var:
            log(module=__name__, function='connect', level='ERROR', msg='Init ERROR !!')
            log(module=__name__, function='connect', level='ERROR', msg='EXCEPTION {}'.format(err_var))
            spyplc_stats.inc_count_errors()
            self.connected = False

        return self.connected

    def init_sysvars_record(self, dlgid):
        '''
        Inicializa el registro del DLGID Solo si no existe !!!!
        '''
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False

        if not self.handle.hexists(dlgid, 'LINE'):              # Guarda la ultima linea de datos recibida
            self.handle.hset(dlgid, 'LINE', 'NUL')
        if not self.handle.hexists(dlgid, 'OUTPUTS'):           # Comandos a enviar por el SPX para controlar las salidas
            self.handle.hset(dlgid, 'OUTPUTS', '-1')
        if not self.handle.hexists(dlgid, 'RESET'):             # Indica al SPX que debe resetearse
            self.handle.hset(dlgid, 'RESET', 'FALSE')
        if not self.handle.hexists(dlgid, 'POUT'):              # Valores de presion a mandar a un DLG c/piloto
            self.handle.hset(dlgid, 'POUT', '-1')
        if not self.handle.hexists(dlgid, 'PSLOT'):             # Valores de timeslots a mandar a un DLG c/pilotos
            self.handle.hset(dlgid, 'PSLOT', '-1')
        if not self.handle.hexists(dlgid, 'MEMFORMAT'):         # Indica al SPX que debe resetear su memoria
            self.handle.hset(dlgid, 'MEMFORMAT', 'FALSE')
        if not self.handle.hexists(dlgid, 'MODBUS'):            # Comandos modbus para ejecutar en el SPX
            self.handle.hset(dlgid, 'MODBUS', 'NUL')
        if not self.handle.hexists(dlgid, 'BROADCAST'):         # Linea con comandos modbus a ejecutar el SPX/PLC
            self.handle.hset(dlgid, 'BROADCAST', 'NUL')
        if not self.handle.hexists(dlgid, 'DLGREMOTOS'):        # Guarda lista de dlg remotos en automatismos
            self.handle.hset(dlgid, 'DLGREMOTOS', 'NUL')
        if not self.handle.hexists(dlgid, 'VALID'):             # Validez del registro. Si no es valido, todo se debe
            self.handle.hset(dlgid, 'VALID', "True")            # releer de BD persistente y recrear el registro
        return True

    def save_statistics(self, pkdict ):
        '''
        Guarda un diccionario de estadisticas de procesamiento del frame en una lista de REDIS
        '''
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False

        self.handle.rpush('LQUEUE_STATS', pkdict)
        return True

    def invalidate_record(self, dlgid):
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False

        self.handle.hset(dlgid, 'VALID', "False")

    def enqueue_data_record(self, d):
        '''
         Encola en LQ_PLCDATA un pickle con los datos.
         Luego un proceso se encargara de generar un INIT record en GDA

         Si la cola LQ_PLCDATA no existe, con el compando rpush se crea automaticamente
         '''
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False

        pkdict = pickle.dumps(d)
        try:
            if self.handle.rpush('LQ_PLCDATA', pkdict):
                return True
        except Exception as err_var:
            log(module=__name__, function='save_data_record',level='ERROR', msg='Init ERROR !!')
            log(module=__name__, function='save_data_record',level='ERROR', msg='EXCEPTION {}'.format(err_var))
            spyplc_stats.inc_count_errors()
        return False

    def get_bcastline(self, dlgid):
        '''
        En el campo BROADCAST está los comandos que mando de respuesta al PLC
        El formato actual es del tipo [2,2223,2,16,FLOAT,C1032,0][3,1234,5,16,FLOAT,C3210,7.34].....
        Esta hecho para que un datalogger genere los comandos modbus necesarios pero el PLC solo necesita el campo
        address y valor
        [add,valor][addr,valor].......
        NO BORRO EL CAMPO BROADCAST.
        '''
        response = ''

        # log(module=__name__, function='get_orders2plc', dlgid=dlgid, msg='REDIS {0}'.format(dlgid))

        if not self.connect():
            log(module=__name__, function='get_bcastline', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            spyplc_stats.inc_count_errors()
            return response

        # Si no hay registro ( HASH) del datalogger lo creo.
        if not self.handle.exists(dlgid):
            self.init_sysvars_record(dlgid)
            return response

        if self.handle.hexists(dlgid, 'BROADCAST'):
            try:
                bcast_line = self.handle.hget(dlgid, 'BROADCAST').decode()
            except:
                log(module=__name__, function='get_bcastline', level='ERROR', dlgid=dlgid, msg='ERROR in HGET{0}'.format(dlgid))
                spyplc_stats.inc_count_errors()
                return response

            log(module=__name__, function='get_bcastline', level='SELECT', dlgid=dlgid,
                msg='BCAST:{}'.format(bcast_line))
            if bcast_line != 'NUL':
                l1 = bcast_line.replace('][', ';')
                l1 = l1.replace('[', '')
                l1 = l1.replace(']', '')
                l2 = [(int(b), float(g)) for (a, b, c, d, e, f, g) in [x.split(',') for x in l1.split(';')]]
                for i, j in l2:
                    response += '{0}:{1};'.format(i, j)
                # Elimino todos los caracteres vacios
                response = response.replace(' ', '')
                log(module=__name__, function='get_bcastline', level='SELECT', dlgid=dlgid,
                    msg='PLC_CMDS:{}'.format(response))

        return response

    def read_lqueue_length(self, queue_name):
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False

        return self.handle.llen(queue_name)

    def lpop_lqueue(self, queue_name, size):
        if not self.connect():
            spyplc_stats.inc_count_errors()
            return False
        lines = []
        try:
            lines = self.handle.lpop(queue_name, size )
        except Exception as err_var:
            log(module=__name__, function='lpop_lqueue', level='ERROR', msg='ERROR Queue={0} !!'.format(queue_name))
            log(module=__name__, function='lpop_lqueue', level='ERROR', msg='Queue={0}, EXCEPTION {1}'.format(queue_name, err_var))
            spyplc_stats.inc_count_errors()
        return lines

    def save_line(self, dlgid, line ):
        # Guardo la ultima linea en la redis porque la uso para los automatismos
        if not self.connect():
            log(module=__name__, function='save_line', level='ERROR', dlgid=dlgid, msg='REDIS NOT CONNECTED')
            spyplc_stats.inc_count_errors()
            return False

        line = 'LINE=' + line
        try:
            self.rh.hset(self.dlgid, 'LINE', line)
            return True
        except Exception as err_var:
            log(module=__name__, function='save_line', dlgid=self.dlgid, msg='ERROR: Redis insert line err !!')
            log(module=__name__, function='save_line', dlgid=self.dlgid, msg='ERROR: EXCEPTION {}'.format(err_var))
            spyplc_stats.inc_count_errors()
            return False
        return False

