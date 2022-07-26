#!/home/pablo/Spymovil/python/pyenv/ml/bin/python3

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
from FUNCAUX import stats
from FUNCAUX.config import Config
from FUNCAUX.log import log


class BD_GDA:

    def __init__(self):
        self.connected = False
        self.conn = None
        self.handle = None
        self.engine = None
        self.url = Config['BDATOS']['url_gda_spymovil']
        self.response = False

    def connect(self):
        if self.connected :
            stats.inc_count_accesos_GDA()
            return True
        # Engine
        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect',level='ERROR',msg='ERROR: engine NOT created. ABORT !!')
            log(module=__name__, function='connect',level='ERROR', msg='ERROR: EXCEPTION {0}'.format(err_var))
            stats.inc_count_errors()
            return False

        # Connection
        try:
            self.conn = self.engine.connect()
            self.connected = True
            stats.inc_count_accesos_GDA()
            return True
        except Exception as err_var:
            self.connected = False
            log(module=__name__, function='connect',level='ERROR',msg='ERROR: BDSPY NOT connected. ABORT !!')
            log(module=__name__, function='connect',level='ERROR',msg='ERROR: EXCEPTION {0}'.format(err_var))
            stats.inc_count_errors()
            return False

        return False

    def exec_sql(self, dlgid, sql):
        # Ejecuta la orden sql.
        if not self.connect():
            log(module=__name__, function='exec_sql',level='ERROR', dlgid=dlgid, msg='ERROR: No hay conexion a BD. Exit !!')
            return None

        try:
            query = text(sql)
        except Exception as err_var:
            log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: SQLQUERY: {0}'.format(sql))
            log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: EXCEPTION {0}'.format(err_var))
            stats.inc_count_errors()
            return

        log(module=__name__, function='exec_sql', level='SELECT', dlgid=dlgid, msg='QUERY={0}'.format(query))
        rp=None
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            '''
            if 'duplicate'.lower() in (str(err_var)).lower():
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, function='exec_sql', level='WARN', dlgid=dlgid, msg='WARN {0}: Duplicated Key'.format(dlgid))
            else:
                log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: {0}, EXCEPTION {1}'.format(dlgid, err_var))
            '''
        return rp

    def insert_dlg_raw(self, dlgid, d ):
        # Inserta la linea tal cual se recibio.
        log(module=__name__, function='insert_dlg_raw',level='SELECT', dlgid=dlgid, msg='Start' )

        # Inserto frame en la tabla de DATA.
        sql = """INSERT INTO dlg_raws (dlgid,fechasys, tipo, rxstr) VALUES ( '{0}', NOW(),'DATA','{1}')""" .format(dlgid, d.get('RCVD','EMPTY_LINE'))
        return self.exec_sql(dlgid, sql)

    def insert_dlg_data(self, dlgid, d ):
        # Inserta cada par key_value
        log(module=__name__, function='insert_dlg_data', level='SELECT', dlgid=dlgid, msg='Start')

        # Inserto frame en la tabla de DATA.
        for key in d:
            if key in ['ID', 'RCVD', 'VER']:
                continue
            value = d.get(key,'None')
            if value != 'None':
                try:
                    fvalue = float(value)
                except:
                    favalue = np.NaN

            sql = """INSERT INTO dlg_data (dlgid,fechadata, tag, value) VALUES ('{0}',NOW(),'{1}','{2}')""" .format(dlgid, key, fvalue)
            self.exec_sql(dlgid, sql)
        return

    def insert_spx_datos(self, dlgid, d ):
        # Inserta cada par key_value
        log(module=__name__, function='insert_spx_datos', level='SELECT', dlgid=dlgid, msg='Start')

        for key in d:
            if key in ['ID', 'RCVD', 'VER']:
                continue
            value = d.get(key, 'None')
            if value != 'None':
                try:
                    fvalue = float(value)
                except:
                    favalue = np.NaN
            sql = """INSERT INTO spx_datos (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES \
                         ( now(),now(),'{0}',( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion \
                         AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id WHERE \
                         cp.parametro = 'NAME' AND cp.value = '{1}' AND u.dlgid = '{2}' ),( SELECT ubicacion_id FROM spx_instalacion \
                         WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{2}')))""".format( fvalue, key, dlgid )
            self.exec_sql(dlgid, sql)
        return

    def insert_spx_datos_online(self, dlgid, d ):
        # Inserta cada par key_value
        log(module=__name__, function='insert_spx_datos_online', level='SELECT', dlgid=dlgid, msg='Start')

        for key in d:
            if key in ['ID', 'RCVD', 'VER']:
                continue
            value = d.get(key, 'None')
            if value != 'None':
                try:
                    fvalue = float(value)
                except:
                    favalue = np.NaN

            sql = """INSERT INTO spx_online (fechasys, fechadata, valor, medida_id, ubicacion_id ) VALUES \
                         ( now(),now(),'{0}',( SELECT uc.tipo_configuracion_id FROM spx_unidades AS u JOIN spx_unidades_configuracion \
                         AS uc ON uc.dlgid_id = u.id JOIN spx_configuracion_parametros AS cp  ON cp.configuracion_id = uc.id WHERE \
                         cp.parametro = 'NAME' AND cp.value = '{1}' AND u.dlgid = '{2}' ),( SELECT ubicacion_id FROM spx_instalacion \
                         WHERE unidad_id = ( SELECT id FROM spx_unidades WHERE dlgid = '{2}')))""".format( fvalue, key, dlgid )
            self.exec_sql(dlgid, sql)
        return

    def read_dlg_conf(self, dlgid):
        '''
        Leo la configuracion desde GDA
                +----------+---------------+------------------------+----------+
                | canal    | parametro     | value                  | param_id |
                +----------+---------------+------------------------+----------+
                | BASE     | RESET         | 0                      |      899 |
                | BASE     | UID           | 304632333433180f000500 |      899 |
                | BASE     | TPOLL         | 60                     |      899 |
                | BASE     | COMMITED_CONF |                        |      899 |
                | BASE     | IMEI          | 860585004331632        |      899 |

                EL diccionario lo manejo con 2 claves para poder usar el metodo get y tener
                un valor por default en caso de que no tenga alguna clave
        '''
        log(module=__name__, function='read_dlg_conf', level='SELECT', dlgid=dlgid, msg='start')

        sql = """SELECT spx_unidades_configuracion.nombre as canal, spx_configuracion_parametros.parametro, 
                    spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                    spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                    WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                    AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                    AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                    AND spx_unidades.dlgid = '{}'""".format (dlgid)

        rp = self.exec_sql(dlgid, sql)
        results = rp.fetchall()
        d = {}
        log(module=__name__, function='read_dlg_conf', dlgid=dlgid, level='SELECT', msg='Reading conf from GDA.')
        for row in results:
            canal, pname, value, *pid = row
            d[(canal, pname)] = value
            log(module=__name__, function='read_dlg_conf', dlgid=dlgid, level='INFO', msg='BD conf: [{0}][{1}]=[{2}]'.format( canal, pname, d[(canal, pname)]))

        '''
        El pasaje de la configuracion es por medio del diccionario d_conf de los argumentos' \
        Lo primero es ponerlo en blanco.
        Y luego le copio el actual con la configuracion
        '''
        return d

    def get_d_reenvios(self, dlgid):
        '''
        Lee la configuracion de los reenvios.
        '''
        log(module=__name__, function='get_d_reenvios', level='SELECT', dlgid=dlgid, msg='start')

        sql = """SELECT dlg, medida, rem_mbus_slave, rem_mbus_regaddress, tipo,codec
                FROM spx_reenvio_modbus as rm INNER JOIN spx_unidades as u ON rm.dlgid_id = u.id
                WHERE u.dlgid = '{}'""".format(dlgid)
        rp = self.exec_sql(dlgid, sql)
        results = rp.fetchall()
        log(module=__name__, function='get_d_reenvios', dlgid=dlgid, level='SELECT', msg='Reading conf from GDA.')

        if results is None:
            log(module=__name__, function='get_d_reenvios', dlgid=dlgid, level='SELECT', msg='No hay equipos remotos')
            return None

        # Armo un dict con keys que sean tuplas (dlgid, medida)
        d = {}
        for row in results:
            #log(module=__name__, server=self.server, function='get_dlg_remotos', dlgid=dlgid, level='SELECT', msg="ROW={}".format(row))
            dlg_rem, medida, mbus_slave, mbus_regaddr, tipo,codec = row
            #log(module=__name__, server=self.server, function='get_dlg_remotos', dlgid=dlgid, level='SELECT', msg="dlg_rem={0},medida={1}, mbus_slave={2}, mbus_regaddr={3}, tipo={4},codec={5}".format(dlg_rem, medida, mbus_slave, mbus_regaddr, tipo, codec))
            #d[dlg_rem, medida] = dict()
            #d[dlg_rem,medida]['MBUS_SLAVE'] = mbus_slave
            #d[dlg_rem,medida]['MBUS_REGADDR'] = mbus_regaddr
            #d[dlg_rem,medida]['TIPO'] = tipo
            #d[dlg_rem,medida]['CODEC'] = codec
            if dlg_rem not in d:
                d[dlg_rem] = {}
            d[dlg_rem][medida] = {'MBUS_SLAVE': mbus_slave, 'MBUS_REGADDR': mbus_regaddr, 'TIPO': tipo, 'CODEC': codec}

        log(module=__name__, function='get_d_reenvios', dlgid=dlgid, level='SELECT', msg='D_REMOTOS={0}'.format(d))
        return d

