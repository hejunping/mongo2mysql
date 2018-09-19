# -*- coding:utf-8 -*-
"""
description: 从mongoDB中同步指定的字段对Mysql数据库
author: rick
date: 2018/09/18
"""
import datetime
import sys
from Mongo2MysqlHandle import Mongodata
reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    mid = None
    gapday = -1    # 0 (当天数据) -n (N天前数据) 99 (初始化数据)
    parm = sys.argv
    if len(parm) > 1:
        mid = parm[1]
    if len(parm) > 2:
        gapday = int(parm[2])
    stime = datetime.datetime.now()
    print u"开始时间:%s" % stime.strftime("%Y-%m-%d %H:%M:%S")
    Mongodata(mid, gapday)
    etime = datetime.datetime.now()
    print u"结束时间:%s" % etime.strftime("%Y-%m-%d %H:%M:%S")
    print u"总用时:%s" % (etime-stime)
