from engine import *
import time


# engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'base.test', ),
#                         connect_args={"charset": "utf8"}, echo=False, )

def is_B0(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B1(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=1 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B2(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=2 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B4(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=4 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B3(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=3 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B5(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=5 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
# def is_B8(JR,statistic_date):
#     return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=8 where fund_id='{}' and statistic_date ='{}' ".format(JR, statistic_date))

def is_B8(JR,statistic_date):
    return engine_data_test.execute("INSERT INTO fund_nv_data_standard_test (fund_id,statistic_date, is_abnormal)  VALUES ('{}' ,'{}' ,8)".format(JR, statistic_date))

def is_B9(JR,statistic_date):
    return engine_data_test.execute("INSERT INTO fund_nv_data_standard_test (fund_id,statistic_date, is_abnormal)  VALUES ('{}' ,'{}' ,9)".format(JR, statistic_date))

# def is_B9(JR,statistic_date):
#     return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=9 where fund_id='{}' and statistic_date='{}' ".format(JR, statistic_date))

def to_remark(remark,JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_test set remark='{}' where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(remark,JR, statistic_date))

def crawl(list):


    for JR in list:
        nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id = '{}'".format(JR),engine_base)
        # nav=nv["nav"]
        # added_nav=nv["added_nav"]
        # time=nv["statistic_date"]
        if (nv["nav"] == nv["added_nav"]).all():
            try:
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
                to_sql("fund_nv_data_standard_test", engine_data_test, df, type="update")#----------------------------------------输入1的异常
                print("异常")
                print(JR)

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
            except BaseException:
                pass
            else:
                pass


        else:
            #
            c2=nv.loc[nv["nav"]!=nv["added_nav"]]

            try:
                c2["nnn"] = round((c2["added_nav"] - c2["nav"]),3)
                c2["ccc"] = round((c2["added_nav"] / c2["nav"]),3)

                cnt = c2["nnn"].value_counts()
                ccc = c2["ccc"].value_counts()
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


def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv


def check():
    df=pd.read_sql("SELECT t.fund_id,t.statistic_date,t.is_abnormal,b.nav FROM fund_nv_data_standard_test as t LEFT JOIN base.fund_nv_data_standard as b ON t.fund_id=b.fund_id and \
     t.statistic_date=b.statistic_date AND t.is_abnormal in (1,2,3,4,5) WHERE b.nav is not NULL",engine_data_test)
    # df=pd.read_sql("select fund_id,statistic_date,is_abnormal,")

    # df["statistic_date"]=df["statistic_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    df["nav"] = df["nav"].apply(lambda x: '%.4f' % x)
    x=to_list(df)
    for i in x:
        JR=i[0]
        time=i[1]
        nav=i[3]
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
            pass
        else:
            pass

from multiprocessing.dummy import Pool as ThreadPool

def len_JR():
    JR_all = pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard", engine_base)
    # JR_all = pd.read_sql("SELECT DISTINCT fund_id from fund_nv_data_standard where nav<0.1", engine_base)
    bb = np.array(JR_all)  # np.ndarray()
    vv = bb.tolist()  # list
    ALL=[]
    for i in vv:
        for o in i:
            ALL.append(o)

    a=len(ALL)
    b=int(a/10)
    all = []
    for i in range(b):
        a=ALL[i*10:(i*10+10)]
        all.append(a)
    all.append(ALL[b*10:])
    return all




# all=len_JR()
# pool = ThreadPool(20)
# pool.map(crawl,all)
# pool.close()
# pool.join()


check()
print("DOWN"+'\n'+now2)








