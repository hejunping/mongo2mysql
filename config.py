#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 控制是否输出警告日志,不喜欢的可以改为 False
log_switch = False
# 控制是否输出信息日志,不喜欢的可以改为 False
info_switch = True

# function use secs
use_time_switch = True

# debug info
debug_switch = True

# 测试环境正式环境
env = "test"

# 生成文件的存储路
project_apth = os.path.abspath(os.path.dirname(__file__))
filepath = project_apth + "/file/"

# 定义mongog的数据源
mongo_db_info = (
    "mongodb://xxx:xxx@xxx.xxx.xxx.xxx:27017/db?authSource=db&authMechanism=SCRAM-SHA-1",
)

# 定义mysql的数据源
mysql_db_info = {
    "pro": ("xxx.xxx.xxx.xxx", "user", "password", "dbname", "utf8", 3306),
    "test": ("xxx.xxx.xxx.xxx", "user", "password", "dbname", "utf8", 3306)
}

