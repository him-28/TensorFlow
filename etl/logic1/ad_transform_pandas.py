# encoding=utf-8
'''
使用Pandas处理CSV文件数据
'''
import os
import numpy as np
import pandas as pd
import datetime as dt
import logging
import sys

from etl.util import init_log
LOG = init_log.init("util/logger.conf", 'pandasEtlLogger')


import yaml

PLACEHOLDER = -9

def split_header(names, header):
    '''转换配置里的数据类型、列名'''
    target_dtype = {}
    for name in names:
        the_type = header[name]
        if the_type == 'int':
            target_dtype[name] = np.int64
        elif the_type == 'string':
            target_dtype[name] = np.string0
    return names, target_dtype


class AdTransformPandas(object):
    """使用Pandas处理CSV文件数据"""
    def __init__(self, console_print=False):
        '''初始化'''
        LOG.info("Welcome to AdTransformPandas")
        if console_print:  # print debug info in console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            fmt_str = "[%(levelname)s] %(asctime)s [%(name)s] [%(funcName)s] %(message)s"
            console_handler.setFormatter(logging.Formatter(fmt_str, ""))
            LOG.addHandler(console_handler)
        # 参数
        self.params = {}

    def calculate(self, input_path, input_filename, alg_file):
        '''计算数据
        @param alg_file: 统计指标、输出文件路径
        @param input_path: 输入路径
        @param input_filename: 输入文件名
        @return: -1 失败 0 成功
        '''
        exec_start_time = dt.datetime.now()  # 开始执行时间
        if not isinstance(alg_file, dict):
            LOG.error("非法参数alg_file：" + str(alg_file))
            return -1
        # handle path
        input_path = input_path.replace("\\", os.sep).replace("/", os.sep)
        if not input_path.endswith(os.sep):
            input_path = input_path + os.sep
        # calculate
        for algorithm in alg_file.iterkeys():
            calcu_result = self.__calculate(algorithm, alg_file[algorithm], \
                                             input_path, input_filename)
            if calcu_result == -1:  # 出错
                LOG.error("calculate [" + algorithm + "] failed.")
        exec_end_time = dt.datetime.now()  # 结束执行时间
        exec_takes = exec_end_time - exec_start_time

        LOG.info("all task process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        print "all task process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)"
        return 0

    def merge_section(self):
        ''' 计量各分段数据总合 '''
        # info
        LOG.info("merge the tmp file : " + self.__get('tmp_file_path'))
        # 输出结果到文件
        dataframe = pd.read_csv(self.__get('tmp_file_path'), sep=self.__get('output_column_sep'), \
                         names=self.__get("sum_names"), header=None, dtype=self.__get('dtype'))
        dataframe.dropna()  # 去除NaN行
        total_grouped = dataframe.groupby(self.__get('group_item')).sum()

        # merge info
        LOG.info("merge result size:" + str(len(total_grouped)))
        total_groupframe = pd.DataFrame(total_grouped)


        LOG.info("remove tmp file:" + self.__get('tmp_file_path'))
        # 删除临时文件
        os.remove(self.__get('tmp_file_path'))
        return total_groupframe

    def __calculate(self, trans_type, output_file_path, input_path, input_filename):
        ''' 计算一个维度的数据 '''
        self.__configure(trans_type, output_file_path, input_path, input_filename)

        exec_start_time = dt.datetime.now()  # 开始执行时间
        try:
            is_load_success = self.__load_file()
            if is_load_success:
                merge_result = self.merge_section()
                self.__save_result_file(merge_result)
                self.__insert(merge_result)
            else:
                LOG.error("load file failed,the file may not exists,exit.")
                return -1
        except Exception, exce:
            import traceback
            print traceback.format_exc()
            LOG.error(exce.message)
            return -1
        exec_end_time = dt.datetime.now()  # 结束执行时间

        exec_takes = exec_end_time - exec_start_time

        LOG.info("process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        return 0

    def __configure(self, trans_type, output_file_path, input_path, input_filename):
        '''配置参数'''
        cnf = yaml.load(file("logic1/ad_config.yml"))
        self.__put(("input_file_path", "output_file_path"), \
                    (input_path + input_filename, output_file_path))
        self.__put("column_sep", cnf.get("column_sep"))
        self.__put("output_column_sep", cnf.get("output_column_sep"))
        self.__put(("names", "dtype"), split_header(cnf.get("names"), cnf.get("header")))
        self.__put(("chunk", "db_chunk"), (cnf.get("read_csv_chunk"), cnf.get("db_commit_chunk")))
        self.__put("tmp_file_path", output_file_path + ".tmp")
        self.__configure_algorithm(trans_type, cnf)
        LOG.info("configure complete, retrieve params:" + str(self.params))

    def __configure_algorithm(self, trans_type, cnf):
        '''各个维度的配置'''
        algorithm = cnf.get("algorithm")
        trans_type_cnf = algorithm.get(trans_type)
        self.__put("condition", trans_type_cnf.get("condition"))
        self.__put("group_item", trans_type_cnf.get("group_item"))
        sum_names = []
        for item in self.__get('group_item'):
            sum_names.append(item)
        for key in self.__get('condition').keys():
            sum_names.append(key)
        self.__put("sum_names", sum_names)

    def __load_file(self):
        '''加载文件并计算'''
        input_file_path = self.__get("input_file_path")
        LOG.info('load file:' + input_file_path)
        if os.path.exists(input_file_path):
            # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
            data_chunks = pd.read_csv(input_file_path, sep=self.__get('column_sep'), \
                            names=self.__get('names'), dtype=self.__get('dtype'), \
                            header=None, chunksize=self.__get('chunk'), index_col=False)
            self.__transform_section(data_chunks)
        else:
            return False
        return True


    def __transform_section(self, data_chunks):
        '''分段转换数据'''
        LOG.info('transform...')
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############

        if os.path.exists(self.__get('tmp_file_path')):
            LOG.info("tmp_file exists,remove : " + self.__get('tmp_file_path'))
            os.remove(self.__get('tmp_file_path'))

        for chunk in data_chunks:
            chunk_len = len(chunk)  # 记录chunk长度
            grouped = None
            condition = self.__get("condition")
            for cdt_key in condition.iterkeys():
                grouped = self.__calculate_algorithm(grouped, cdt_key, condition[cdt_key], chunk)

            if not grouped is None:
                groupframe = pd.DataFrame(grouped)
                # 保存到临时CSV文件
                groupframe.to_csv(self.__get('tmp_file_path'), \
                                   sep=self.__get('output_column_sep'), \
                                 header=False, na_rep='0', mode="a")
            elif not os.path.exists(self.__get('tmp_file_path')):  # 创建一个空文件
                tmp_file = os.open(self.__get('tmp_file_path'), os.O_CREAT)
                os.close(tmp_file)
            # info info
            LOG.info("grouped: " + str(chunk_len) + " records")

        LOG.info("save to tmp file : " + self.__get('tmp_file_path'))
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
        LOG.info ('transform!')

    def __calculate_algorithm(self, grouped, column_name, relations, tmp_chunk):
        '''根据关系规则Group分段数据，把分段Group结果合并'''
        LOG.info("filter column: " + column_name)
        for rel in relations:
            key = rel[0]
            opt = rel[1]
            val = rel[2]
            if '==' == opt:
                LOG.info("filter column: " + key + "==" + str(val))
                tmp_chunk = tmp_chunk[tmp_chunk[key] == val]
            elif '!=' == opt:
                LOG.info("filter column: " + key + "!=" + str(val))
                tmp_chunk = tmp_chunk[tmp_chunk[key] != val]
            elif "in" == opt:
                LOG.info("filter column: " + key + " in " + str(val))
                tmp_chunk = tmp_chunk[tmp_chunk[key].isin(val)]
        LOG.info("merge column result: " + column_name)

        if len(tmp_chunk) == 0:
            return None

        if column_name == 'count':
            if grouped is None:
                grouped = tmp_chunk.groupby(self.__get('group_item')).size()
            else:
                grouped = pd.concat([grouped, tmp_chunk.\
                                    groupby(self.__get('group_item')).size()], axis=1)
        else:
            LOG.error("unsupport operation:" + column_name)
            return None
        return grouped

    def __save_result_file(self, merge_result):
        '''保存计算结果到文件'''
        #merge_result = merge_result[merge_result[0] != PLACEHOLDER]
        LOG.info("save result to csv file:" + self.__get('output_file_path'))
        merge_result.to_csv(self.__get('output_file_path'), sep=\
                self.__get('output_column_sep'), header=False)

    def __insert(self, merge_result):
        '''处理数据插入数据库'''
        pass

    def __put(self, key, value):
        '''设置self.params'''
        if isinstance(key, tuple):
            __len = len(key)
            for i in range(0, __len):
                __key = key[i]
                __value = value[i]
                self.__put(__key, __value)
        else:
            self.params[key] = value

    def __get(self, key):
        '''获取self.params'''
        return self.params[key]

