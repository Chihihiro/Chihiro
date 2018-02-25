import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
import time
import numpy as np
from engine import *
now = time.strftime("%Y-%m-%d")
now1=time.time()
print(now1)

df=pd.read_excel('C:\\Users\\63220\\Desktop\\fund_id11-13.xls')
print(df)
train_data = np.array(df)#np.ndarray()
cc=train_data.tolist()#list
print(cc)
DF=[]
for c in cc:
    fund_ID=c[0]
    source_ID=c[1]
    print(fund_ID)
    df_max=pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date,nav FROM fund_nv_data_source WHERE fund_id IN ('{}') and data_source<>3 ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id".format(fund_ID),engine_base)
    cl_max=pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date,nav FROM d_fund_nv_data WHERE fund_id IN ('{}') and source_code=3 ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id".format(source_ID),engine_crawl)
    date=df_max['statistic_date'].tolist()
    cl=cl_max['statistic_date'].tolist()
    cl2=cl_max.iloc[0,2]
    str_date = cl2.strftime("%Y-%m-%d")
    str_date2 = str_date + ' 00:00:00'
    cl_timeArray = time.strptime(str_date2, "%Y-%m-%d %H:%M:%S")
    cl_timeStamp = int(time.mktime(cl_timeArray))
    print(cl_timeStamp)
    if cl_timeStamp<0:
        pass
    elif len(df_max)==0:
        y = (fund_ID, '3', '1')
        DF.append(y)
    else:
        date2 = df_max.iloc[0, 2]
        # print(type(date2))
        str_date = date2.strftime("%Y-%m-%d")
        str_date2 = str_date + ' 00:00:00'
        # print(str_date2)
        # print(type(str_date2))
        timeArray = time.strptime(str_date2, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        # print(timeStamp)
        if cl_timeStamp>=timeStamp:
            print('采用新源')
            x=(fund_ID,'3','1')
            DF.append(x)
        else:
            pass
df_all=pd.DataFrame(DF)

df_all.columns =["fund_id","data_source","is_updata"]
print(df_all)
is_checked = input("输入1来确认入库\n")
if is_checked == "1":
    engine_TEST = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
    to_sql("fund_nv_updata_source", engine_TEST, df_all, type="update")
else:
    pass



