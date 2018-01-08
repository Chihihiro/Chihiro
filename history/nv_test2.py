import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
from engine import engine_base
import numpy as np
from engine import *


engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                        connect_args={"charset": "utf8"}, echo=False, )
# to_sql("market_info", engine_base, dataframe, type="update")
def is_B0(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B1(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=1 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B2(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=2 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B4(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=4 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B3(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=3 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B5(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=5 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B8(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=8 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B9(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=9 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def crawl():
    JR_all=pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard",engine5)

    bb = np.array(JR_all)#np.ndarray()
    vv=bb.tolist()#list
    for I in vv:
        JR=I[0]
    # JR = 'JR000898'
        nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id = '{}'".format(JR),engine5)
        nav=nv["nav"]
        added_nav=nv["added_nav"]
        time=nv["statistic_date"]
        if (nv["nav"] == nv["added_nav"]).all():
            diff = abs(nv[["statistic_date", "nav", "added_nav"]] - nv[["statistic_date", "nav", "added_nav"]].shift(1))
            diff = diff[1:]
            diff["statistic_date"] = diff["statistic_date"].apply(lambda x: x.days)
            diff.columns = ["days","nav","added_nav"]
            time_1 = nv["statistic_date"]
            diff["nca"] = (nv["nav"] - nv["nav"].shift(1)) / nv["nav"].shift(1)
            diff["nca2"] = (nv["nav"] - nv["nav"].shift(2)) / nv["nav"].shift(2)
            # diff["dca"] = nv["statistic_date"] - nv["statistic_date"].shift(1)
            difff=diff.join(time_1)#总表
            dif = difff.loc[(difff["nca"].abs() > 0.1) & (difff["nca2"].abs() > 0.12)]
            # dif["fund_id1"] = JR
            cc = pd.DataFrame(dif)
            cc["fund_id"] = JR
            cc["is_abnormal"] = 1
            df = cc[["fund_id", "is_abnormal","statistic_date"]]
            to_sql("fund_nv_data_standard", engine5, df, type="update")#----------------------------------------输入1的异常

            print("异常1")
            print(JR)

            # dis1 = difff.loc[(difff["days"] >= 5) & ((difff["nca"].abs() > 0.12) | (difff["nca2"].abs() > 0.12))]


            bb = np.array(dif)  # np.ndarray()
            aa = bb.tolist()  # list
            num = len(aa)
            for i in range(num):
                day=aa[i][0]
                bo = abs(aa[i][4])
                stat = str(aa[i][5])

                if day<=3 and bo<0.15:
                    is_B2(JR,stat)
                    print("小小波")
                    pass
                elif 5>=day>3 and bo < 0.2:
                    is_B3(JR, stat)
                    print("小波")
                    pass

                elif 14>=day>=6 and bo<0.25:
                    is_B4(JR, stat)
                    print("中波动")

                elif day>=21 and bo<0.5:
                    is_B5(JR, stat)
                    print("大波动月更")

                else:
                    is_B1(JR, stat)
                    # try:
                    #     bon=abs((aa[i-1]-aa[i])/aa[i-1])
                    #     statistic_date_next = time_all[i-1]
                    #     bonn = abs((aa[i +1] - aa[i]) / aa[i + 1])
                    #     statistic_date_next1 = time_all[i + 1]
                    #     if abs(bon-bo)<0.05:
                    #         engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                    #                             ='{}' ".format(JR, statistic_date_next))
                    #         print("前面一条改0")
                    #     elif abs(bonn-bo)<0.05:
                    #         engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                    #                                                         ='{}' ".format(JR, statistic_date_next1))
                    #         print("后面一条改0")
                    #     # else:
                    #     #     engine5.execute("update fund_nv_data_standard set is_abnormal=3 where fund_id='{}' and statistic_date\
                    #     #      ='{}' ".format(JR, statistic_date))
                    # except BaseException:
                    #     print("有错")
                    #     pass
                    # else:
                    #     engine5.execute("update fund_nv_data_standard set is_abnormal=1 where fund_id='{}' and statistic_date\
                    #                                                                                 ='{}' ".format(JR,statistic_date))
                    #     print("最后标1")
        else:
            c1=nv.loc[nv["nav"]==nv["added_nav"]]
            c2=nv.loc[nv["nav"]!=nv["added_nav"]]
            c_1=c1[["fund_id","statistic_date"]]
            # c_1["is_abnormal"]="0"
            # to_sql("fund_nv_data_standard", engine5, c_1, type="update")
            try:
                c2["nnn"] = (c2["nav"] - c2["added_nav"]).round(4)
                c2["ccc"] = (c2["added_nav"] / c2["nav"]).round(4)
                cnt = c2["nnn"].round(4).value_counts()
                ccc = c2["ccc"].round(4).value_counts()
                # cnt = c2["nnn"].value_counts()
                # cnt[cnt < cnt.mean()]
                y = cnt[cnt <= 1].index
                z = ccc[ccc <= 1].index
                Y=len(y)
                Z = len(z)
                # if y.empty:
                if Z > Y:
                    if y.empty:
                        pass
                    else:
                        for i in y:
                            a = c2[c2.nnn == i]
                            bb = np.array(a)  # np.ndarray()
                            aa = bb.tolist()  # list
                            JR=aa[0][0]
                            stat=str(aa[0][2])
                            is_B8(JR,stat)
                            print(JR)
                            print("分红错")
                elif Z <Y:
                    if z.empty:
                        pass
                    else:
                        for i in z:
                            a = c2[c2.ccc == i]
                            bb = np.array(a)  # np.ndarray()
                            aa = bb.tolist()  # list
                            JR=aa[0][0]
                            stat=str(aa[0][2])
                            is_B9(JR,stat)
                            print(JR)
                            print("拆分错")
                else:
                    pass

            except BaseException:
                pass
            else:
                pass


# df=pd.read_sql("SELECT fund_id,statistic_date,nav FROM fund_nv_data_standard where fund_id in\
#                 (select  fund_id FROM fund_nv_data_standard WHERE is_abnormal is NOT NULL GROUP BY fund_id HAVING COUNT(*)<=3)\
#                 and is_abnormal in (1,2,3,4,5)",engine5)

df=pd.read_sql("SELECT fund_id,statistic_date,nav FROM fund_nv_data_standard where is_abnormal in (1,2,3,4,5)  and remark is NULL ",engine5)

import datetime

import time

df["statistic_date"]=df["statistic_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
df["nav"] = df["nav"].apply(lambda x: '%.4f' % x)

def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv

x=to_list(df)


# x=[['JR000002', '2015-05-22', 1.721], ['JR000002', '2015-07-03', 1.237]]


def to_remark(remark,JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set remark='{}' where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(remark,JR, statistic_date))



for i in x:
    JR=i[0]
    time=i[1]
    nav=i[2]
    try:
        df=pd.read_sql("SELECT fund_id,data_source,data_source_name,nav,added_nav\
                        from fund_nv_data_source where fund_id='{}' and statistic_date='{}'".format(JR,time),engine_base)
        df["nav"] = df["nav"].apply(lambda x: x / 100 if x > 60 else x)
        df["nav"] = df["nav"].apply(lambda x: '%.4f' % x)
        num=len(df)
        if num>=2:
            c = df[df.nav!=nav]
            nc=len(c)
            if c.empty:
                remark="全对"
                print(remark)
                to_remark(remark,JR,time)

            else:
                y=str(round((1-nc/num)*100,1))
                print(y)
                to_remark(y,JR, time)

        else:
            pass
    except BaseException:
        print("错误")
    else:
        pass


print("DOWN")

































