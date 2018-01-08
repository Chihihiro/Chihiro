from WindPy import w as wind
from sqlalchemy import Column, String, create_engine
import os
import pandas as pd
import shutil
import sys
import re
from openpyxl import load_workbook
import time
import pymysql
import datetime,time
from iosjk import *
import time
now = time.strftime("%Y-%m-%d")


# fileobj = open('C:\\Users\\63220\\Desktop\\wind新月.txt','r')
# try:
#     strings = fileobj.read()
# finally:
#     fileobj.close()
# wind_id= strings.split('\n')
# print(wind_id)
#
#
#
# wind.start()
# for id in wind_id:
#     # tmp=wind.wsd(id, "NAV_date", "2004-01-01",now, "Period=Y") # 年
#     tmp = wind.wsd(id, "NAV_date", "2017-01-01", now, "Period=M") #月
#     df = pd.DataFrame(tmp.Data)   # 用结果(list)去构造一个DataFrame二维表
#     df=df.T
#     df=df.dropna()
#     print(df)
#     a=str(df.iloc[0,0])
#     a1=a[0:10]
#     b=str(df.iloc[-1,0])
#     b1=b[0:10]
#     print(a1)
#     print(b1)
#     wind_ruru=[id,a1,b1]
#     print(wind_ruru)
#     # exit()
#     x=pd.DataFrame(wind_ruru)
#     x=x.T
#     x.columns=["wind_id","start_date","end_date"]
#     print(x)
#     dataframe=x
#     # engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_4', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
#     engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
#     to_sql("wind_date", engine, dataframe, type="update")
# #
# exit()



engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),connect_args={"charset": "utf8"}, echo=True, )

fund_ids=pd.read_sql('select wind_id from wind_date where update_time like "2017-11-03 %%"',engine)
dds=fund_ids["wind_id"].tolist()
print(dds)
# vv=fund_ids.tolist()

# aa=str(fund_ids)
# print(aa)

# # print(fund_ids)

for dd in dds:



    start=pd.read_sql('select start_date from wind_date WHERE wind_id LIKE "{fid}"'.format(fid=dd), engine).iloc[0,0].strftime("%Y-%m-%d")
    print(start)
    # print(fund_id)
    fund_name = pd.read_sql('select fund_name from fund_info WHERE fund_id IN (select fund_ID from fund_id_match WHERE source_ID LIKE "{fid}")'.format(fid=dd), engine2).iloc[0, 0]
    print(fund_name)

    wind.start()
    tmp=wind.wsd(dd, "NAV_date,nav,NAV_acc,NAV_adj",start,now, "Period=W")
    df = pd.DataFrame(tmp.Data)
    df=df.T
    df.columns=["statistic_date","nav","added_nav","adjusted_nav"]
    # df=df.drop_duplicates(['statistic_date'])
    df['source_id'] = "020007"
    df['fund_name'] = fund_name
    df['fund_id'] = dd
    df['adjusted_nav'] = df['adjusted_nav'].apply(lambda x: '%.4f' % x)
    dataframe = df.drop_duplicates(['statistic_date'])
    print(dataframe)
    to_sql("d_fund_nv", engine3, dataframe, type="update")
    # is_checked = input("输入1来确认入库\n")
    # if is_checked == "1":
    #     # engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
    #     to_sql("d_fund_nv", engine3, dataframe, type="update")
    # else:
    #     pass




