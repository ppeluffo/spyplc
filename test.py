#!/opt/anaconda3/envs/mlearn/bin/python3

import os

rows = [ ('PABLO1', 'H', '1', '1962', 'FLOAT', 'c0123'), ('PABLO2', 'H', '2', '1963', 'FLOAT', 'c3210'), ('PABLO3', 'H', '3', '1964', 'FLOAT', 'c3210'), ('PABLO1', 'pA', '1', '1965', 'FLOAT', 'c0123'), ('PABLO2', 'pB', '2', '1966', 'i16', 'c3210') ]


if __name__ == '__main__':

    d=dict()
    for row in rows:
        dlg_rem, medida, mbus_slave, mbus_regaddr, tipo, codec = row
        if not dlg_rem in d:
            d[dlg_rem] = {}
        d[dlg_rem][medida] = {'MBUS_SLAVE': mbus_slave, 'MBUS_REGADDR': mbus_regaddr, 'TIPO': tipo, 'CODEC': codec}

        #d[dlg_rem][medida] = {}
        d1={}
        d1['MBUS_SLAVE'] = mbus_slave
        d1['MBUS_REGADDR'] = mbus_regaddr
        d1['TIPO'] = tipo
        d1['CODEC'] = codec
        d2 = {}
        d2[medida] = d1
        d[dlg_rem]


        d2={medida:dt}
        d[dlg_rem][medida]=d2






