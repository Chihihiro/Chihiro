from WindPy import w as wind
from sqlalchemy import create_engine
import pandas as pd
import re
import time
from engine import *

wind.start()  # 启动wind
now = time.strftime("%Y-%m-%d")

wind_ids = pd.read_sql("select DISTINCT fund_id from d_fund_nv WHERE source_id=020007 and fund_id not like 'JR%%'",
                       engine_crawl_private)
dds = wind_ids["fund_id"].tolist()
print(dds)

ids = ['XT1705118.XT',
    'XT1705155.XT',
    'XT1705158.XT',
    'XT1705163.XT',
    'XT1705164.XT',
    'XT1705165.XT']


tmp = wind.wsd("XT1702906.XT", "nav,NAV_acc,NAV_date", "2015-09-01", "2018-07-09", "Period=W")

df = pd.DataFrame(tmp.Data)  # 用结果(list)去构造一个DataFrame二维表
df = df.T  # 把表转置
print(df)

df_dr = df.dropna(how="any", axis=1)
