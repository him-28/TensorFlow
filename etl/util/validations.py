#encoding=utf-8

import petl as etl

supply_header = ('type', 'boardid', 'deviceid', 'voideoid', 'slotid', 'cardid', 'creativeid', 'p_v_hid', 'p_v_rid',
        'p_v_rname', 'p_c_type', 'p_c_ip', 'cityid', 'intime', 'p_c_idfa', 'p_c_imei', 'p_c_ctmid', 'p_c_mac', 'p_c_anid',
        'p_c_openudid', 'p_c_odin', 'p_c_aaid', 'p_c_duid', 'sid')
demand_header = ('slotid', 'cardid', 'creativeid', 'deviceid', 'type', 'intime', 'ip', 'boardid', '_sid', 'voideoid', 'second')

"""
 x.da.hunantv.com 请求数据's validations
"""
supply_constraints = [
        dict(name='type_int', field='type', test=int),
        dict(name='boardid_int', field='boardid', test=int),
        dict(name='deviceid_int', field='deviceid', test=int),
        dict(name='voideoid_int', field='voideoid', test=int),
        dict(name='slotid_int', field='slotid', test=int),
        dict(name='cardid_int', field='cardid', test=int),
        dict(name='creativeid_int', field='creativeid', test=int),
        dict(name='p_v_hid_int', field='p_v_hid', test=int),
        dict(name='p_v_rid_int', field='p_v_rid', test=int),
        dict(name='p_v_rname_int', field='p_v_rname', test=int),
        #dict(name='p_c_type', field='', test=),
        #dict(name='p_c_ip', field='', test=),
        #dict(name='cityid', field='', test=),
        #dict(name='intime', field='', test=),
        #dict(name='p_c_idfa', field='', test=),
        #dict(name='p_c_imei', field='', test=),
        #dict(name='p_c_ctmid', field='', test=),
        #dict(name='p_c_mac', field='', test=),
        #dict(name='p_c_anid', field='', test=),
        #dict(name='p_c_openudid', field='', test=),
        #dict(name='p_c_odin', field='', test=),
        #dict(name='p_c_aaid', field='', test=),
        #dict(name='p_c_duid', field='', test=),
        #dict(name='sid', field='', test=),
        ]

"""
" 上报数据's validations
"""
demand_constraints = [
        dict(name='slotid_int', field='slotid', test=int),
        dict(name='cardid_int', field='cardid',test=int),
        dict(name='creativeid_int', field='creativeid',test=int),
        #dict(name='deviceid', field='',test),
        dict(name='type_int', field='type',test=int),
        #dict(name='intime', field='',test),
        #dict(name='ip', field='',test),
        dict(name='boardid_int', field='boardid',test=int),
        #dict(name='_sid', field='',test),
        dict(name='voideoid_int', field='voideoid',test=int),
        #dict(name='second', field='',test),
        dict(name='not_none', assertion=lambda row: None not in row)
        ]

def ad_validations(tab):
    problems = etl.validate(tab, constraints=demand_constraints, header=demand_header)
    print etl.nrows(problems)
    problems.lookall()

"""
statistics error rows
"""
def statistics():
    pass

if __name__ == '__main__':
    table = etl.fromcsv('/Users/martin/Documents/03-raw data/20150923.04.product.demand.csv')
    table1 = etl.split(table,'67\t990\t669\tmgtvmac1048B14F6247\t1\t1442952002\t175.150.183.141\t4820\tnull\t1739456\t0', '\t', demand_header)
    ad_validations(table1)
