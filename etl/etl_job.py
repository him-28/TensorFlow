# encoding=utf-8
'''
Created on 2015年10月13日

@author: dico
'''
from etl_transform_pandas import Etl_Transform_Pandas
import etl_transform
import day_etl_transform
import sys
import datetime as dt

if __name__ == '__main__':
    start_time = dt.datetime.strptime(sys.argv[1], "%Y%m%d")
    end_time = dt.datetime.strptime(sys.argv[2], "%Y%m%d")

    days = (end_time - start_time).days


    task1_start_time = dt.datetime.now()
    ############# run task 1
    for i in range(0, days):
        date = dt.datetime.strftime(start_time + dt.timedelta(i), "%Y%m%d")  # 每次递增一天
        for j in range(0, 24):
            hour = "%02d" % j
            print ("##########:exec:hour_etl('" + date + "', '" + hour + "', 'hour', 'old')")
            # etl_transform.hour_etl(date, hour, 'hour', 'old')  # 小时
        print ("##########:exec:day_etl('" + date + "', 'merge', 'old')")
        # day_etl_transform.day_etl(date, "merge", "old")
    task1_end_time = dt.datetime.now()
    task1_takes = (task1_end_time - task1_end_time).seconds
    print "T1T1T1T1T1T1T1: task1 takes " + str(task1_takes) + " seconds ("+str(task1_takes/60)+")."


    ############# run task 2
    task2_start_time = dt.datetime.now()
    for i in range(0, days):
        date = dt.datetime.strftime(start_time + dt.timedelta(i), "%Y%m%d")  # 每次递增一天
        for j in range(0, 24):
            hour = "%02d" % j
            ############# run task 1
            date_hour = date + "." + hour
            print ("##########:exec:Etl_Transform_Pandas('supply_hour_hit', '" + date_hour + "')")
            Etl_Transform_Pandas('supply_hour_hit', date_hour)
            print ("##########:exec:Etl_Transform_Pandas('supply_hour_reqs', '" + date_hour + "')")
            Etl_Transform_Pandas('supply_hour_reqs', date_hour)
            print ("##########:exec:Etl_Transform_Pandas('demand_hour_ad', '" + date_hour + "')")
            Etl_Transform_Pandas('demand_hour_ad', date_hour)

        print ("##########:exec:Etl_Transform_Pandas('supply_day_hit', '" + date + "',True)")
        Etl_Transform_Pandas('supply_day_hit', date, True)
        print ("##########:exec:Etl_Transform_Pandas('supply_day_reqs', '" + date + "',True)")
        Etl_Transform_Pandas('supply_day_reqs', date, True)
        print ("##########:exec:Etl_Transform_Pandas('demand_day_ad', '" + date + "',True)")
        Etl_Transform_Pandas('demand_day_ad', date, True)
    task2_end_time = dt.datetime.now()
    task2_takes = (task2_end_time - task2_end_time).seconds
    print "T2T2T2T2T2T2T2: task2 takes " + str(task2_takes) + " seconds ("+str(task2_takes/60)+")."