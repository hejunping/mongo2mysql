#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time         : 18/8/31 10:20 
# @Author       : Rick
# @Site         :  
# @File         : util.py 
# @Description  : PyCharm

import config
import time
import datetime
import functools
import emoji


def log(func):
    def log_wrapper(*args, **kwargs):
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if config.info_switch:
            print ("%s-消息:start-%s:%s:%s" % (ctime, func.__name__, str(args), str(kwargs)))
        if config.log_switch:
            print ("%s-日志:%s:%s" % (ctime, str(args), str(kwargs)))
        value = func(*args, **kwargs)
        if config.info_switch:
            print ("%s-消息:end-%s:%s:%s" % (ctime, func.__name__, str(args), str(kwargs)))
        if config.log_switch:
            print ("%s-日志:%s:%s" % (ctime, str(args), str(kwargs)))
        return value

    return log_wrapper


def timer(func):
    """ Print the runtime of the decorated function """

    @functools.wraps(func)
    def timer_wrapper(*args, **kwargs):
        if config.use_time_switch:
            start_time = time.time()
            value = func(*args, **kwargs)
            end_time = time.time()
            run_time = end_time - start_time
            print "Finished %s in %.2f secs" % (func.__name__, run_time)
        else:
            value = func(*args, **kwargs)
        return value

    return timer_wrapper


def debug(func):
    """ Print the function signature and return value"""

    @functools.wraps(func)
    def debug_wrapper(*args, **kwargs):
        if config.debug_switch:
            args_repr = [repr(a) for a in args]
            kwargs_repr = ["%s=%s" % (k, v) for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            value = func(*args, **kwargs)
            print("%s returned %s" % (func.__name__, signature))
        else:
            value = func(*args, **kwargs)
        return value

    return debug_wrapper


def string2timestamp(strValue):
    """ 转化时间戳 """
    d = datetime.datetime.strptime(strValue, "%Y-%m-%d %H:%M:%S.%f")
    t = d.timetuple()
    timeStamp = int(time.mktime(t))
    timeStamp = str(timeStamp) + str("%06d" % d.microsecond)
    return int(timeStamp[0:13])


def timestamp2string(timestamp, format="%Y-%m-%d %H:%M:%S"):
    """ 把时间戳转化成时间格式 """
    timestamp = str(timestamp)
    datastr = "0000-00-00 00:00:00"
    if len(timestamp) == 13:
        timestamp = int(int(timestamp) / 1000)
        datastr = time.strftime(format, time.localtime(timestamp))
    elif len(timestamp) == 10:
        datastr = time.strftime(format, time.localtime(int(timestamp)))
    return datastr


def substrbyindex(str, delim='_', index=0):
    """分割字符串返回指定"""
    index = int(index)
    rt = ''
    ls = str.split(delim)
    if index < len(ls):
        rt = ls[index]
    return rt


def _replace(str, skey, rkey):
    """替换字符串"""
    for k in skey.split("|"):
        str = str.replace(k, rkey)
    return str


def _substring(str, end, start=0):
    """分割字符"""
    return str[start:int(end)]


def remove_emoji(val):
    """删除字符串中的表情"""
    return emoji.demojize(val)


def _escape(var):
    """转义回车符号"""
    return var.replace("\n", "\\n").replace("\t", "\\t")
