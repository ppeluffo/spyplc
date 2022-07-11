#!/opt/anaconda3/envs/mlearn/bin/python3
'''
Clase que genera frames con valore aleatorios para testing.
Lee las magnitudes definidas para el datalogger de modo que los frames no den error al insertarlos en GDA
'''

from spyplc_bd_gda import BD_GDA
import numpy as np
import urllib.request


class SENDFRAMES:

    def __init__(self):
        self.url = None
        self.response = None
        self.dlgid = 'DEFAULT'
        self.server = '127.0.0.1'
        self.script = 'spyplc.py'
        self.path = 'SPYPLC'
        self.port = 80
        self.fw_ver = '4.0.4b'
        self.verbose = True
        self.payload_template = ''
        self.payload = ''

    def set_dlgid(self,dlgid):
        self.dlgid = dlgid

    def set_server(self,server):
        self.server = server

    def set_port(self, port):
        self.port = port

    def set_fw_ver(self, fw_ver):
        self.fw_ver = fw_ver

    def set_script(self, script):
        self.script = script

    def set_path(self, path):
        self.path = path

    def set_verbose(self, verbose):
        self.verbose = verbose

    def set_frame_list(self, frame_list):
        self.frame_list = frame_list

    def get_params(self):
        print('DLGID = {}'.format(self.dlgid))
        print('SERVER = {}'.format(self.server))
        print('PORT = {}'.format(self.port))
        print('FW_VER: {}'.format(self.fw_ver))
        print('SCRIPT: {}'.format(self.script))
        print('VERBOSE: {}'.format(self.verbose))

    def prepare_random_payload(self):
        # Payload: Genero una al azar
        l_tags = ['PA', 'PB', 'Q0', 'H1', 'H2', 'SEN1', 'BMB0', 'BMB1', 'FS', 'CAU0', 'CAU1', 'IO0', 'IO1', 'IO2','AIN0', 'AIN1']
        nro_tags = np.random.randint(2, len(l_tags))
        tags = np.random.choice( l_tags, nro_tags, replace=False)
        for tag in tags:
            self.payload_template += '%s:REPLACE_VAL;' % (tag)
        self.payload_template += 'bt:12.01;'
        return self.payload_template

    def prepare_payload_template(self):
        '''
        Prepara un payload pero con nombres que el datalogger tenga configurado para que no de error al insertarlo
        en GDA.
        Solo genera el template
        '''
        gda = BD_GDA()
        d = gda.read_dlg_conf(self.dlgid)
        mag_names=[ d[(ch,tag)] for (ch,tag) in d if tag =='NAME' and d[(ch,tag)] not in ['bt','X'] ]
        if len(mag_names) == 0:
            print('ERROR: El dlgid {0} no tienen en la BDGDA magnitudes usables. EXIT !!'.format(self.dlgid))
            exit(1)

        for mag in mag_names:
            self.payload_template += '%s:REPLACE_VAL;'% (mag)
        self.payload_template += 'bt:12.01;'
        return self.payload_template

    def fill_payload(self):
        self.payload = self.payload_template
        # Rellena el template con valores aleatorios.
        while self.payload.find('REPLACE_VAL') > 0:
            new_val = '%0.3f' % (np.random.random() * 100)
            self.payload = self.payload.replace('REPLACE_VAL', new_val, 1)
        return self.payload

    def send(self):
        if self.verbose:
            print('\nFrame:')

        # Header
        self.url = 'http://{0}:{1}/cgi-bin/{2}/{3}?ID:{4};VER:{5};'.format(self.server, self.port, self.path, self.script, self.dlgid, self.fw_ver)
        #
        # Calculo los valores instantaneos y transmito
        self.fill_payload()
        self.url += self.payload
        #
        if self.verbose:
            print('SENT: {0}'.format(self.url))
        # Envio
        try:
            req = urllib.request.Request(self.url)
        except:
            print('\nERROR al enviar frame {0}'.format(self.url))

        print('.',end='', flush=True)

        with urllib.request.urlopen(req) as response:
            self.response = response.read()

        if self.verbose:
            print('RESP: {0}'.format(self.response))
        #

