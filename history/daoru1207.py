import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
from engine import engine_base
import numpy as np
import time

engine3 = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),
    connect_args={"charset": "utf8"}, echo=True, )
engine_base = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
    connect_args={"charset": "utf8"}, echo=True, )
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,
                                                                'config_private', ), connect_args={"charset": "utf8"},
                        echo=True, )
engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                        connect_args={"charset": "utf8"}, echo=True, )
# to_sql("market_info", engine_base, dataframe, type="update")


JR=pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard",engine_base)

bb = np.array(JR)#np.ndarray()
vv=bb.tolist()#list


for i in vv:
    jr=i[0]
    nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id like '{}'".format(jr),engine_base)
    to_sql("fund_nv_data_standard", engine5, nv, type="update")
