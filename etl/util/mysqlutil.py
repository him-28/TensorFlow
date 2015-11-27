# encoding=utf-8
'''MySQL DBUtils'''
import os
import MySQLdb

import datetime as dt


from etl.conf.settings import LOGGER

class DBUtils(object):
    """
    insert: DBUtils.insert('insert into [table] values(1,2,3)')
    bulk_insert: DBUtils.bulk_insert('insert into [table] values(%d, %d, %d)', [(1,2,3), (4,5,6)])
    """

    @staticmethod
    def bulk_insert(sql, vals, conn, commit=True, bulk_size=1000):
        '''bulk_insert:
        DBUtils.bulk_insert('insert into [table] values(%d, %d, %d)', [(1,2,3), (4,5,6)])
        '''
        if not isinstance(vals, list):
            return False
        try:
            if not conn:
                return False
            cur = conn.cursor()
            for i in xrange(0, len(vals), bulk_size):
                cur.executemany(sql, vals[i:i + bulk_size])
            if commit:
                conn.commit()
            return True
        except Exception, e:
            import traceback
            traceback.format_exc()
            LOGGER.error('pg bulk insert %s result error: %s' , len(vals), e.message)
            if commit and conn:
                conn.rollback()
            return False
        finally:
            if commit and conn:
                conn.close()
    @staticmethod
    def excute_by_sql(str_sql, conn=None, commit=True):
        '''excute sql'''
        result = None
        try:
            if not str_sql:
                return
            if not conn:
                conn = DBUtils.get_connection()
            cur = conn.cursor()
            result = cur.execute(str_sql)
            conn.commit()
        except Exception, exc:
            LOGGER.error("excute sql:%s error,error message:%s", str_sql, exc.message)
            if commit and conn:
                conn.rollback()
        finally:
            if commit and conn:
                cur.close()
                conn.close()
        return result

    @staticmethod
    def clear(table_name, clear_cols, clear_conditions, conn=None, commit=True):
        '''delete datas'''
        if not clear_conditions or not len(clear_conditions):
            return None
        if not clear_cols or not len(clear_cols):
            return None
        if len(clear_conditions) != len(clear_cols):
            return None
        where_condition = " "
        for i in range(0, len(clear_cols)):
            where_condition += " "
            where_condition += clear_cols[i]
            where_condition += " = "
            if isinstance(clear_conditions[i], int):
                where_condition += str(clear_conditions[i])
            if isinstance(clear_conditions[i], str):
                where_condition += "'{0}'".format(clear_conditions[i])
            where_condition += " and "
        where_condition += "1=1"
        clear_sql = "delete from  {0} where {1}".format(table_name, where_condition)
        try:
            DBUtils.excute_by_sql(clear_sql, conn, commit)
            return True
        except Exception, exc:
            LOGGER.error('excute clear sql:%s error: %s' , clear_sql, exc.message)
        return False

    @staticmethod
    def get_connection(database, user, passwd, host, port=3306):
        '''return mysql db connection'''
        conn = None
        try:
            conn = MySQLdb.connect(db=database, user=user, passwd=passwd, host=host, port=port)
        except Exception , exc:
            LOGGER.error('get mysql connection error,error message: %s', exc.message)
        return  conn

SQL_INSERT_QUERY = 'INSERT INTO %s (%s) VALUES (%s)'

class LoadUtils(object):
    '''Load file to db'''
    @staticmethod
    def fromCsvtodb(filepath, tablename, conn, cols=None, \
                    commit=False, bulkread_size=1000, split_char='\t',
                    skip_first_row=False, cols_type=None):
        '''
        commit = false 表示全部插入成功后在提交，否则表示每次插入都会提交
        cols = None 表示 columns从文件中读取，否则以给定list作为columns
        '''
        if not filepath or not os.path.exists(filepath):
            LOGGER.error("file path not exists:'%s'" , filepath)
            raise Exception("file path not exists:'%s'" % filepath)
        try:
            read_buffer = []
            is_first_row = True
            with open(filepath, 'rb') as filelines:
                for line in filelines:
                    row = [i.strip() for i in line.strip().split(split_char)]
                    if is_first_row:
                        is_first_row= False
                        if not cols:
                            cols = row
                            continue
                        if skip_first_row:
                            continue
                    if cols_type:
                        for col,col_type in cols_type.iteritems():
                            col_idx = cols.index(col)
                            if col_type == 'timestamp':
                                if row[col_idx]:
                                    row[col_idx] = dt.datetime.fromtimestamp(float(row[col_idx]))
                    read_buffer.append(row)
                    if len(read_buffer) >= bulkread_size:
                        LoadUtils.todb(read_buffer, conn, tablename, cols, commit)
                        read_buffer = []
            if len(read_buffer) > 0:
                LoadUtils.todb(read_buffer, conn, tablename, cols, commit)
            if not commit:
                conn.commit()
        except Exception, exc:
            LOGGER.error("load file to db error,filepath: %s ,message: %s" , filepath, exc.message)
            import traceback
            ex = traceback.format_exc()
            LOGGER.error(ex)
            if not commit and conn:
                conn.rollback()
        finally:
            if not commit and conn:
                conn.close()

    @staticmethod
    def todb(table, conn, tablename, cols, commit):
        '''insert values'''
        _cols = [col for col in cols]
        colnames = ','.join(_cols)
        _placeholders = ','.join(['%s'] * len(cols))
        sql = SQL_INSERT_QUERY % (tablename, colnames, _placeholders)
        LOGGER.info("insert %s records: %s", len(table), sql)
        try:
            DBUtils.bulk_insert(sql, table, conn, commit)
        except Exception, exc:
            LOGGER.error("insert to db error,message:'%s'" , exc.message)
            raise exc
