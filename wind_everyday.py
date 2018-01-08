from WindPy import w as wind
from sqlalchemy import Column, String, create_engine
import pandas as pd
import re
import datetime,time
from iosjk import to_sql

wind.start()    # 启动wind
now = time.strftime("%Y-%m-%d")

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),connect_args={"charset": "utf8"}, echo=True, )

wind_ids=pd.read_sql('select DISTINCT fund_id from d_fund_nv WHERE source_id=020007 ',engine3)
dds=wind_ids["fund_id"].tolist()
print(dds)


def crawl_benchmark(id):    # 定义方法
    tmp = wind.wsd(id, "NAV_date,nav,NAV_acc,NAV_adj,fund_fullname", "ED", now, "")# 万得API用法
    a=wind.wsd("111058.SZ", "creditrating,amount,rate_latest", "2014-04-19", "2017-12-25", "")
    df = pd.DataFrame(tmp.Data)   # 用结果(list)去构造一个DataFrame二维表
    df = df.T  # 把表转置
    print(df)
    A=str(df.iloc[0,0])
    if A =="None":
        R="None"
        B = str(df.iloc[0, 1])
        C = str(df.iloc[0, 2])
        D = df.iloc[0, 3]
        E = df.iloc[0, 4]
        return (id,R, B, C,D,E)
    else:
        r = re.findall(r"(.+?) ", A)
        R = r[0]
        B = str(df.iloc[0, 1])
        C = str(df.iloc[0, 2])
        D = df.iloc[0, 3]
        E = df.iloc[0, 4]
        return (id,R, B, C,D,E)


# x='XT1703378.XT'
# q=crawl_benchmark(x)
# print(q)

    # wdid=('XT045313.XT','XT1703499.XT',	'XT1703378.XT',	'XT1703377.XT',)
DF=[]
DF_SOURCE=[]
for i in dds:
    q=crawl_benchmark(i)
    print(q)
    if q[3] =="None":
        pass
    else:
        QQ = ('020007','0')
        qq=q+QQ
        DF.append(qq)
        cc=pd.read_sql("select fund_ID from fund_id_match WHERE source_ID='{}'".format(i),engine2).iloc[0,0]
        sqq = ('3','第三方','13','wind',cc)
        source = q+sqq
        DF_SOURCE.append(source)


df = pd.DataFrame(DF)
df2 = pd.DataFrame(DF_SOURCE)
df.columns = ["fund_id","statistic_date", "nav", "added_nav","adjusted_nav","fund_name","source_id","is_del"]
df2.columns = ["wind_id", "statistic_date", "nav", "added_nav", "swanav", "fund_name", "source_code", "source","data_source","data_source_name","fund_id"]
del df2["wind_id"]
del df2["swanav"]
df['adjusted_nav'] = df['adjusted_nav'].apply(lambda x: '%.4f' % x)
print(df)
print(df2)
dataframe=df
dataframe2 = df2



to_sql("d_fund_nv", engine3, dataframe, type="update")
to_sql("fund_nv_data_source", engine2, dataframe2, type="update")

