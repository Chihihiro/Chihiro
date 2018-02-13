import pandas as pd
import xlrd
from iosjk import to_sql
from sqlalchemy import create_engine
from engine import *
import numpy as np
engine_base = create_engine(
            "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
            connect_args={"charset": "utf8"}, echo=True, )

def to_ku(DF):
    if DF.empty:
        print("df为空")
    else:
        engine_base = create_engine(
            "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
            connect_args={"charset": "utf8"}, echo=True, )

        engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                                connect_args={"charset": "utf8"}, echo=True, )
        to_sql("fund_org_mapping", engine_base, DF, type="update")

def org_type(DF,code):
    if code==1:
        DF["org_type_code"] = "1"
        DF["org_type"] = "投资顾问"
        return DF
    elif code==2:
        DF["org_type_code"] = "2"
        DF["org_type"] = "基金管理人"
        return DF
    else:
        print("code，df 错")

df=pd.read_sql("SELECT * FROM id_match as a \
                LEFT JOIN (SELECT fund_id,MAX(VERSION),fund_name_amac,fund_issue_org_amac,manage_type_amac FROM crawl_private.x_fund_info_private where VERSION>=2017112719 GROUP BY fund_id) as b\
                on a.source_id=b.fund_id\
                WHERE a.source in (010003) AND a.is_used=1 and a.matched_id not in (select fund_id from fund_org_mapping where org_type_code=2)",engine_base)
###----------------------------------------------------private 最新


# df=pd.read_sql("SELECT a.matched_id,a.source,a.source_id,b.fund_id,b.fund_name_amac,b.fund_issue_org_amac,b.manage_type_amac FROM id_match as a \
#                 LEFT JOIN (SELECT fund_id,MAX(VERSION),fund_name_amac,fund_issue_org_amac,manage_type_amac FROM crawl_private.x_fund_info_private\
#                  GROUP BY fund_id) as b on a.source_id=b.fund_id\
#                 WHERE a.source in (010003) AND a.is_used=1 and a.matched_id \
#                 in (SELECT fund_id FROM fund_info);",engine_base) #private 全量
#
#
#
df2=pd.read_sql("SELECT * FROM id_match as a \
                LEFT JOIN (SELECT fund_id,MAX(VERSION),fund_name_amac,fund_issue_org_amac FROM crawl_private.x_fund_info_fundaccount  GROUP BY fund_id) as b\
                on a.source_id=b.fund_id WHERE a.source in (010002) AND a.is_used=1 and a.matched_id\
                in (SELECT fund_id FROM fund_info); -- funt_account全量",engine_base)



df2=pd.read_sql("SELECT * FROM id_match as a  \
            LEFT JOIN (SELECT fund_id,MAX(VERSION),fund_name_amac,fund_issue_org_amac FROM crawl_private.x_fund_info_fundaccount \
            GROUP BY fund_id) as b\
            on a.source_id=b.fund_id WHERE a.source in (010002) AND a.is_used=1 and a.matched_id\
            not in (select fund_id from fund_org_mapping where org_type_code=2);-- 最新",engine_base)



table2=pd.read_sql("select org_id,org_full_name from org_info where org_full_name is not NULL and org_id not like 'JG%%'",engine_base)
table2["org_full_name"]=table2["org_full_name"].apply(lambda x: x.strip())
dict2 ={key:value for key,value in zip(table2["org_full_name"],table2["org_id"])}

table1=pd.read_sql("select org_id,org_name from org_info WHERE org_id not like 'JG%%'",engine_base)
table1["org_name"]=table1["org_name"].apply(lambda x: x.strip())
dict1 ={key:value for key,value in zip(table1["org_id"],table1["org_name"])}

def clean(df):
    df["fund_issue_org_amac"]=df["fund_issue_org_amac"].apply(lambda x: x.strip())
    df["org_id"]=df["fund_issue_org_amac"].apply(lambda x: dict2.get(x))
    df["org_name"]=df["org_id"].apply(lambda x: dict1.get(x))
    del df["fund_id"]
    df.rename(columns={"fund_name_amac": "fund_name", "matched_id": "fund_id"}, inplace=True)
    DF_all=df.dropna(subset=['org_id'], how='all')
    DF = DF_all.drop_duplicates()
    return DF


def fund_private(df):
    DF=clean(df)
    df1=DF[DF["manage_type_amac"]=="自我管理"]
    df2=DF[DF["manage_type_amac"]=="顾问管理"]
    df3=DF[DF["manage_type_amac"]=="受托管理"]
    DF11=df1[["fund_id","fund_name","org_id","org_name"]]
    DF12=df1[["fund_id","fund_name","org_id","org_name"]]
    DF2=df2[["fund_id","fund_name","org_id","org_name"]]
    DF3=df3[["fund_id","fund_name","org_id","org_name"]]
    a=org_type(DF11,1)
    b=org_type(DF12,2)
    c=org_type(DF2,1)
    d=org_type(DF3,2)
    to_ku(a)
    to_ku(b)
    to_ku(c)
    to_ku(d)


def fund_fund_account(df2):
    DF=clean(df2)
    A=DF[["fund_id","fund_name","org_id","org_name"]]
    a=org_type(A,2)
    print(a)
    to_ku(a)





fund_fund_account(df2)
fund_private(df)


print("DOWN")
