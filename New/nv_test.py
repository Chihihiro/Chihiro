import pandas as pd
from History.iosjk import to_sql
from sqlalchemy import create_engine
import numpy as np

engine3 = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),
    connect_args={"charset": "utf8"}, echo=False, )
# engine_base = create_engine(
#     "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
#     connect_args={"charset": "utf8"}, echo=True, )
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,
                                                                'config_private', ), connect_args={"charset": "utf8"},
                        echo=True, )
engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                        connect_args={"charset": "utf8"}, echo=False, )
# to_sql("market_info", engine_base, dataframe, type="update")
def is_B0(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B1(JR,statistic_date):
    return engine5.execute("update fund_nv_data_standard set is_abnormal=1 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))


JR_all=pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard",engine5)

bb = np.array(JR_all)#np.ndarray()
vv=bb.tolist()#list
import datetime
# JR='JR000001'

# for I in vv:
#     JR=I[0]
JR = 'JR000898'
nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id = '{}'".format(JR),engine5)
# nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id IN ('JR000001', 'JR000002')",engine5)
# u = nv.groupby(["statistic_date", "fund_id"])["nav"].last().unstack()
# r = u / u.shift(1) - 1
nav=nv["nav"]
added_nav=nv["added_nav"]
time=nv["statistic_date"]
if (nv["nav"] == nv["added_nav"]).all():
    bb = np.array(nav)  # np.ndarray()
    aa = bb.tolist()  # list
    cc = np.array(time)  # np.ndarray()
    time_all = cc.tolist()  # list
    num = len(aa) - 1


    diff = abs(nv[["statistic_date", "nav", "added_nav"]] - nv[["statistic_date", "nav", "added_nav"]].shift(1))
    diff = diff[1:]
    diff["statistic_date"] = diff["statistic_date"].apply(lambda x: x.days)
    diff.columns = ["days","nav","added_nav"]
    time_1 = nv["statistic_date"]
    diff["nca"] = (nv["nav"] - nv["nav"].shift(1)) / nv["nav"].shift(1)
    diff["nca2"] = (nv["nav"] - nv["nav"].shift(2)) / nv["nav"].shift(2)
    # diff["dca"] = nv["statistic_date"] - nv["statistic_date"].shift(1)
    difff=diff.join(time_1)#总表
    dif = difff.loc[(difff["nca"].abs() > 0.1) & (difff["nca2"].abs() > 0.1)]
    # dif["fund_id1"] = JR
    cc = pd.DataFrame(dif)
    cc["fund_id"] = JR
    cc["is_abnormal"] = 1
    df = cc[["fund_id", "is_abnormal","statistic_date"]]
    to_sql("fund_nv_data_standard", engine5, df, type="update")#----------------------------------------输入1的异常

    dis1 = difff.loc[(difff["days"] >= 5) & ((difff["nca"].abs() > 0.12) | (difff["nca2"].abs() > 0.12))]
    #
    # dif = difff.loc[(difff["days"] < 8) & (difff["days"] > 4) & (difff["nca"].abs() < 0.1)]
    # dif = difff.loc[(difff["days"] < 8) & (difff["days"] > 4) & (difff["nca"].abs() < 0.3)]

    for i in range(num):
        bo=abs((aa[i]-aa[i+1])/aa[i])
        statistic_date=time_all[i]
        dd = datetime.timedelta(1)
        dn1=statistic_date.strftime('%Y-%m-%d')
        dn = datetime.datetime.strptime(dn1, '%Y-%m-%d')
        statistic_date_mt = time_all[i+1] #m明天
        # print(statistic_date_mt)
        dm1 = statistic_date_mt.strftime('%Y-%m-%d')
        dm = datetime.datetime.strptime(dm1, '%Y-%m-%d')
        is_bo=pd.read_sql("select is_abnormal from fund_nv_data_standard WHERE fund_id='{}' and statistic_date\
                            ='{}' ".format(JR,statistic_date),engine5)
        if is_bo.iloc[0, 0] == 0:
            print("之前标0")
            pass
        else:
            if bo<0.1:
                engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                ='{}' ".format(JR,statistic_date))
            else:
                if (dm-dn)>dd:
                    max=(dm-dn).days
                    if 4<max<8 and bo<0.3:
                        engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
                        print("小小波")
                    elif 1.9<max<3.2 and bo<0.2:
                        engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                                        ='{}' ".format(JR, statistic_date))
                        print("小波的")
                    elif max>14 and bo<0.25:
                        engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                                        ='{}' ".format(JR, statistic_date))
                        print("中波动")
                    elif max>25 and bo<0.5:
                        engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                                        ='{}' ".format(JR, statistic_date))
                        print("大波动月更")
                #     # engine5.execute("update fund_nv_data_standard set is_abnormal=2 where fund_id='{}' and statistic_date\
                #     #                              ='{}' ".format(JR, statistic_date))
                #     pass
                else:
                    try:
                        bon=abs((aa[i-1]-aa[i])/aa[i-1])
                        statistic_date_next = time_all[i-1]
                        bonn = abs((aa[i +1] - aa[i]) / aa[i + 1])
                        statistic_date_next1 = time_all[i + 1]
                        if abs(bon-bo)<0.05:
                            engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                ='{}' ".format(JR, statistic_date_next))
                            print("前面一条改0")
                        elif abs(bonn-bo)<0.05:
                            engine5.execute("update fund_nv_data_standard set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                                            ='{}' ".format(JR, statistic_date_next1))
                            print("后面一条改0")
                        # else:
                        #     engine5.execute("update fund_nv_data_standard set is_abnormal=3 where fund_id='{}' and statistic_date\
                        #      ='{}' ".format(JR, statistic_date))
                    except BaseException:
                        print("有错")
                        pass
                    else:
                        engine5.execute("update fund_nv_data_standard set is_abnormal=1 where fund_id='{}' and statistic_date\
                                                                                                    ='{}' ".format(JR,statistic_date))
                        print("最后标1")
else:
    # pass
    c1=nv.loc[nv["nav"]==nv["added_nav"]]
    c2=nv.loc[nv["nav"]!=nv["added_nav"]]
    c_1=c1[["fund_id","statistic_date"]]
    c_1["is_abnormal"]="0"
    to_sql("fund_nv_data_standard", engine5, c_1, type="update")
    c2["nnn"] = (c2["nav"] - c2["added_nav"])
    n = c2[["statistic_date", "nnn"]]
    bb = np.array(n)  # np.ndarray()
    aa = bb.tolist()
    num = aa.__len__() - 1

    for i in range(num):
        statistic_date=aa[i][0]
        n_0=aa[i][1]
        n_1=aa[i+1][1]
        if n_0==n_1:
            is_B0(JR, statistic_date)
        else:
            try:
                f1=aa[i-1][1]
                if f1==n_0:
                    is_B0(JR, statistic_date)
                else:
                    is_B0(JR,statistic_date)
            except BaseException:
                pass
            else:
                is_B0(JR, statistic_date)



























