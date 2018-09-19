# -*- coding:utf-8 -*-
"""
description: 从mongoDB中同步指定的字段对Mysql数据库
author: rick
date: 2018/09/18
"""
import os
import datetime
import MySQLdb
import pymongo
import sys
import util
import config
import demjson
reload(sys)
sys.setdefaultencoding('utf-8')


class Mongodata:

    def __init__(self, mid=None, gapday=-1):
        """初始化数据"""
        # 配置ID
        self.mid = mid
        # 访问数据库
        self.mysql_db = config.mysql_db_info[config.env]
        # 访问数据库连接
        self.mysql_conn = MySQLdb.connect(self.mysql_db[0], self.mysql_db[1], self.mysql_db[2], self.mysql_db[3],
                                          charset=self.mysql_db[4], port=self.mysql_db[5])
        # 访问数据库游标
        self.mysql_cursor = self.mysql_conn.cursor()

        # 存储数据的连接
        self.mysql_to_conn = None

        # 存储数据的游标
        self.mysql_to_cursor = None

        # mongo 数据源
        self.mongo_sorce = None

        # 存储数据源
        self.mysql_sorce = None

        # mongo 数据链接
        self.mongo_client = None

        # mongo 表
        self.mongo_table = None

        # 存储mysql 对应的表
        self.mysql_table = None

        # Mongo 与mysql 的字段映射关系
        self.columns = None

        # 数据同步的条件
        self.condition = None

        #文件存储地址
        self.filepath = config.filepath

        #文件名
        self.filename = None

        # mysql字段
        self.mysqlcolums = None

        # 同步时间差
        self.gapday = gapday

        # start
        self.start()

    @util.timer
    @util.debug
    def get_config(self):
        """
        获取配置信息
        查询Mysql获取基本信息
        """
        if not id:
            raise Exception("Missing parameters ID")
        sqlstr = "SELECT mongo_sorce, mysql_sorce, mongo_tb, mysql_tb, `colums`, `condition` " \
                 "FROM dw_etl_mongo_config_t WHERE id = %s" % self.mid
        self.mysql_cursor.execute(sqlstr)
        result = self.mysql_cursor.fetchone()
        if result:
            self.mongo_sorce = result[0]
            self.mysql_sorce = result[1]
            self.mongo_table = result[2]
            self.mysql_table = result[3]
            self.columns = demjson.decode(result[4])
            self.condition = result[5]
            self.filename = "%s%s_mongo2mysql.txt" % (self.filepath, result[3])
        else:
            raise Exception("no result")

    def set_mongo_info(self):
        """ 设置MongoDB相关配置 """
        if self.mongo_sorce is not None and self.mongo_sorce < len(config.mongo_db_info):
            uri = config.mongo_db_info[self.mongo_sorce]
            self.mongo_client = pymongo.MongoClient(uri)
        else:
            raise Exception(u"mongo配置信息错误")

    def set_mysql_info(self):
        """ 设置 mysql 连接配置 """
        if self.mysql_sorce is not None:
            mysql_db_name = ['caracal', 'wildcat']
            mysql_info = config.mysql_db_info[mysql_db_name[self.mysql_sorce]]
            self.mysql_to_conn = MySQLdb.connect(mysql_info[0], mysql_info[1], mysql_info[2], mysql_info[3],
                                                 charset=mysql_info[4], port=mysql_info[5])
            self.mysql_to_cursor = self.mysql_to_conn.cursor()

    def set_condition(self):
        """
            mongo的筛选条件
            把条件中包含的审计字段替换
            start_date_tamp 换成开始时间
            end_date_tamp 换成结束时间
        """
        start_date = "%s 00:00:00.000" % \
                     (datetime.datetime.now() + datetime.timedelta(days=self.gapday)).strftime("%Y-%m-%d")
        start_date_tamp = util.string2timestamp(start_date)
        end_date = "%s 23:59:59.999" % \
                   (datetime.datetime.now() + datetime.timedelta(days=self.gapday)).strftime("%Y-%m-%d")
        end_date_tamp = util.string2timestamp(end_date)
        if self.condition:
            self.condition = self.condition\
                .replace("start_date_tamp", str(start_date_tamp))\
                .replace("end_date_tamp", str(end_date_tamp))
            self.condition = demjson.decode(self.condition)

    def set_mysql_colums(self):
        """
            对mysql字段解析
            获取对应的Mysql字段
            从如:[["_id","id"], ["resumeVersionId","resumeVersionId"], ["basicInfo.currentCity","currentCityId"]]
            解析成:["id", "resumeVersionId", "currentCityId"]
        """
        self.mysqlcolums = []
        for v in self.columns:
            self.mysqlcolums.append(v[1])

    def set_mongo_colums(self, item):
        """
            对Mongo字段解析
            保存的字段格式为 [["_id","id"], ["resumeVersionId","resumeVersionId"], ["basicInfo.currentCity","currentCityId"]]
            其中数组中的第个元素“_id” 为mogno 中的字段 ,第二个元素 “id” 是同步到Mysql的对应字段
            如果在Mongo中存在多维对象,用对象的形式显示如:
                basicInfo.currentCity
                获取 basicInfo 对象下的curretnCity 值
            支持无限层对象的显示
        """
        filestr = ""
        for v in self.columns:
            fun_name = v[2] if len(v) > 2 else ""
            cols = v[0].split(".")
            val = ""
            for k, y in enumerate(cols):
                if k == 0:
                    val = item.get(y, {})
                elif k == (len(cols) - 1):
                    val = val.get(y, "")
                else:
                    val = val.get(y, {})
                if fun_name == 'timestamp2string':
                    val = util.timestamp2string(val)
            filestr += '"%s",' % val
        return filestr[:-1]

    @util.timer
    def save_mongo_data(self):
        """
            保存获取到的Mongo数据
            每1000行保存一次
        """
        stb = self.mongo_client[self.mongo_client.get_default_database().name][self.mongo_table]
        results = stb.find(self.condition)
        datalist = list()
        i = 0
        j = 0
        for item in results:
            i += 1
            datalist.append(self.set_mongo_colums(item))
            if i == 1000:
                j += 1
                self.save_file(datalist)
                datalist = list()
                i = 0
        self.save_file(datalist)

    def exec_sql(self, sql=None):
        """ 直接执行Mysql脚本 """
        if sql:
            try:
                self.mysql_to_cursor.execute(sql)
                self.mysql_to_conn.commit()
            except MySQLdb.Error as e:
                print e
                self.mysql_to_conn.rollback()

    def exsits_file(self):
        """
            检查文件是否存在
            已经存在则删除
        """
        if os.path.exists(self.filename):
            os.remove(self.filename)
            print "remove success file:%s " % self.filename
        else:
            print "no such file:%s" % self.filename

    def save_file(self, ls=None):
        """保存为文件"""
        if ls:
            with open(self.filename, "aw") as f:
                f.write("\n".join(ls)+"\n")

    @util.timer
    def load_file(self):
        """ 加载数据到Mysql """
        sql = "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY ',' " \
              "ENCLOSED BY '\"' LINES TERMINATED BY '\n' (%s) " %\
              (self.filename, self.mysql_table, ",".join(self.mysqlcolums))
        self.exec_sql(sql)
        print "load done!"

    def truncate_table(self):
        """ 清空表数据 """
        self.exec_sql("TRUNCATE TABLE %s" % self.mysql_table)

    def start(self):
        """ 开始内容 """
        self.get_config()
        self.set_mongo_info()
        self.set_mysql_info()
        if self.gapday == 99:
            print ("init ...")
            self.condition = {}
            self.truncate_table()
        else:
            self.set_condition()
        self.exsits_file()
        self.set_mysql_colums()
        self.save_mongo_data()
        self.load_file()

# if __name__ == '__main__':
#     mid = None
#     gapday = -1    # 0 (当天数据) -n (N天前数据) 99 (初始化数据)
#     parm = sys.argv
#     if len(parm) > 1:
#         mid = parm[1]
#     if len(parm) > 2:
#         gapday = int(parm[2])
#     stime = datetime.datetime.now()
#     print u"开始时间:%s" % stime.strftime("%Y-%m-%d %H:%M:%S")
#     Mongodata(mid, gapday)
#     etime = datetime.datetime.now()
#     print u"结束时间:%s" % etime.strftime("%Y-%m-%d %H:%M:%S")
#     print u"总用时:%s" % (etime-stime)
