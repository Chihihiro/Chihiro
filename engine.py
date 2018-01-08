import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
import numpy as np



engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
engine_base = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
engine_crawl_private = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),connect_args={"charset": "utf8"}, echo=True, )
engine_crawl = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl', ),connect_args={"charset": "utf8"}, echo=True, )


def to_table(df):
    df.to_csv("C:\\Users\\63220\\Desktop\\Pycharm测试.csv")




def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv


import time

now = time.strftime("%Y-%m-%d")