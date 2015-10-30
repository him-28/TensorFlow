# encoding:utf-8
import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter
from etl.logic0.etl_transform import hour_etl
from etl.audit.quality_audit import QualityAuditRobot
from etl.logic0.etl_transform import hour_etl
from etl.logic0.day_etl_transform import day_etl
from etl.logic1.etl_transform_pandas import Etl_Transform_Pandas
from etl.audit.adlog_format_audit import run_aduit_adlog
from etl.conf.settings import LOGGER

class AdMonitorRunner(object):
    def run(self, time, mode='m'):
        """
        mode in minute, hour, day
        """
        assert type(time) == datetime
        assert mode in ['m', 'h', 'd']

        path = ""
        filename = ""

        if mode == 'm':
            path = "{prefix}/{year}/{month}/{day}/{hour}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day,
                    hour = time.hour
                })
            filename = "{minute}_ad.csv".format(minute=time.minute)

            ngx_path = "{prefix}/{year}/{month}/{day}/{hour}".format({
                    prefix = settings.ngx_prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day,
                    hour = time.hour
                })
            ngx_filename = "{minute}_ad.csv".format(minute=time.minute)

            transform_ngx_log(ngx_path, ngx_filename, path, filename)
            calc_ad_monitor(path, filename)
        elif mode == 'h':
            path = "{prefix}/{year}/{month}/{day}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day
                })
            filename = "{hour}_ad.csv".format(hour=time.hour)
            agg_hour_file(time)

        elif mode == 'd':
            path = "{prefix}/{year}/{month}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month
                })
            filename = "{day}_ad.csv".format(day=time.day)
            agg_day_file(time)

        #calc_ad_monitor(path, filename)

        #if time.minute == 60:
        #    agg_hour_file(time)
        #elif time.hour == 24:
        #    agg_day_file(time)


def run_cli(arguments):
    try:
        run_type = arguments[1]
        args = arguments[2:]
        if run_type == 'audit':
            auditLog(args)
        elif run_type == 'pandas':
            pandasEtl(args)
        elif run_type == 'petl':
            petlEtl(args)
        elif run_type == 'quality':
            qualityAudit(args)
            pass
        else:    
            LOGGER.error("app run_type [{0}] is wrong".format(run_type))
            sys.exit(-1)
    except Exception,e:
        LOGGER.error("run app error,error message:"+e.message)
        sys.exit(-1)

def qualityAudit(args):
    '''Args: '20151016' '09' '''
    if len(args) == 2:
        rob = QualityAuditRobot(args[0],args[1])
        rob.scan()
    else:
        LOGGER.error("run qualityAudit error,wrong args: "+"\t".join(args))

def auditLog(args):
    '''Args: '20151016' '09' '''
    if len(args) == 2:
        run_aduit_adlog(args[0],args[1])
    else:
        LOGGER.error("run audit error,wrong args: "+"\t".join(args))

def pandasEtl(args):
    ''' Pandas etl Args:'day' '20151016' 'merge' 'new' 'False' or 'hour' '20151016' '09' 'new' 'False' '''
    etl_type = args[0]
    if etl_type == 'day' and len(args) == 5:
        pandasEtlByDay(args[1:])
    elif etl_type == 'hour' and len(args) == 5:
        pandasEtlByHour(args[1:])
    else:
        LOGGER.error("run pandas etl error,wrong args: "+"\t".join(args))
        
def petlEtl(args):
    ''' Args:'day' '20151016' 'merge' 'new'  or 'hour' '20151016' '09' 'merge' 'new'  '''
    etl_type = args[0]
    if etl_type == 'day' and len(args) == 4:
        #day,type_t,version
        day_etl(args[1],args[2],args[3])
    elif etl_type == 'hour' and len(args) == 5:
        #day,hour,type_t,version
        hour_etl(args[1],args[2],args[3],args[4])
    else:
        LOGGER.error("run petl error,wrong args: "+"\t".join(args))
    
def pandasEtlByDay(args):
    ''' Args: '20151016'  'merge' 'new' 'False' '''
    start_time = args[0]
    is_merge = True
    if len(args) > 1:
        is_merge = args[1] == 'merge'
    is_console_print = False
    is_old = False
    if len(args) > 2:
        is_old = args[2] == 'old'
    if len(args) > 3:
        is_console_print = args[3] == 'True'
    etp = Etl_Transform_Pandas(is_merge, is_console_print)
    if is_old:
        result1 = etp.compute_old('supply_day_hit', start_time)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute_old('supply_day_reqs', start_time)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute_old('demand_day_ad', start_time)
        if result3 == -1:
            sys.exit(-1)
    else:
        result1 = etp.compute('supply_day_hit', start_time)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute('supply_day_reqs', start_time)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute('demand_day_ad', start_time)
        if result3 == -1:
            sys.exit(-1)
            
def pandasEtlByHour(args):
    ''' Args: '20151016' '09' 'new' 'False' '''
    date = args[0]
    hour = args[1]
    is_old = False
    if len(args) > 2:
        is_old = args[2] == 'old'
    is_console_print = False
    if len(args) > 3:
        is_console_print = args[3] == 'True'
    date_hour = date + "." + hour
    etp = Etl_Transform_Pandas(False, is_console_print)
    if is_old:
        result1 = etp.compute_old('supply_hour_hit', date_hour)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute_old('supply_hour_reqs', date_hour)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute_old('demand_hour_ad', date_hour)
        if result3 == -1:
            sys.exit(-1)
    else:
        result1 = etp.compute('supply_hour_hit', date_hour)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute('supply_hour_reqs', date_hour)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute('demand_hour_ad', date_hour)
        if result3 == -1:
            sys.exit(-1)
if __name__ == '__main__':
    '''
      Args like 20151016 08 
            支持 按审计 ,按petl/pandas 按天，按小时，按新/老版本 etl
       run audit args: 'audit' '20151016' '09'
       run petl args: 'petl' 'day' '20151016' 'merge' 'new'  
                        or 'petl' 'hour' '20151016' '09' 'hour' 'new'
       run pandas args: 'pandas' 'day' '20151016' 'merge' 'new' 'False' 
                       or 'pandas' 'hour' '20151016' '09' 'new' 'False'
    '''
    #audit 20151016 09
    run_cli(sys.argv)
    
