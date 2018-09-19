# -*- coding:utf-8 -*-
"""
description: 每天调度多进程运行
author: rick
date: 2018/09/18
"""
import MySQLdb
import sys
import util
import config as cfg
from Mongo2MysqlHandle import Mongodata
from multiprocessing import Pool
reload(sys)
sys.setdefaultencoding('utf-8')


def get_all():
    """ 获取所有需要运行的 """
    mysql_db = cfg.mysql_db_info[cfg.env]
    mysql_conn = MySQLdb.connect(mysql_db[0], mysql_db[1], mysql_db[2], mysql_db[3], charset=mysql_db[4], port=mysql_db[5])
    mysql_cursor = mysql_conn.cursor()
    ids = []
    sql = "SELECT id FROM dw_etl_mongo_config_t WHERE status = 1"
    mysql_cursor.execute(sql)
    results = mysql_cursor.fetchall()
    for item in results:
        ids.append(item[0])
    return ids

@util.timer
def run(id):
    """ 运行脚本 """
    Mongodata(id)

if __name__ == '__main__':
    # 获取所有运行的IDS
    ids = get_all()
    # 开启多进程同时运行
    p = Pool(4)
    for i in ids:
        p.apply_async(run, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
