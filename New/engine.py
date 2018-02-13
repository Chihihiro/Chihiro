import pandas as pd
import sys

from sqlalchemy import create_engine
import numpy as np

from New.iosjk import *

engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
engine_base = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
engine_crawl_private = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),connect_args={"charset": "utf8"}, echo=True, )
engine_crawl = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl', ),connect_args={"charset": "utf8"}, echo=True, )
engine_base_public = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base_public', ),connect_args={"charset": "utf8"}, echo=True, )

def to_table(df):
    df.to_csv("C:\\Users\\63220\\Desktop\\Pycharm测试{}.csv".format(now2))




def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv


import time

now = time.strftime("%Y-%m-%d")
now2 = time.strftime("%Y%m%d%H%M")

def now(a=0):
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=a)
    n_days = now + delta
    print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
    f=n_days.strftime('%Y-%m-%d')
    return f





import datetime
def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    return dates