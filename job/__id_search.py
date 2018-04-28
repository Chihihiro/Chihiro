# _*_ coding: utf-8 _*_
from job.tieba import *
import random
from engine import *
import datetime

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                       connect_args={"charset": "utf8"}, echo=True, )
engine3 = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),
    connect_args={"charset": "utf8"}, echo=True, )


def baseN(num, b):
    return ((num == 0) and "0") or (baseN(num // b, b).lstrip("0") + "0123456789abcdefghijklmnopqrstuvwxyz"[num % b])


# 17121000000003

def reverse_baseN(string, b):
    _sign = "0123456789abcdefghijklmnopqrstuvwxyz"
    if len(string) == 1:
        return _sign.index(string)
    return _sign.index(string[-1]) + reverse_baseN(string[:-1], b) * b


# print(reverse_baseN("62d56zbue",36))  #test
# exit()

def jiami_all(num):
    kaitou = 'P'
    xx = kaitou + baseN(num, 36)
    return xx



import time

now = time.strftime("%Y-%m-%d")


# now = '20180131'
def bian():
    x = time.strftime("%Y-%m-%d")
    # x='20180123'

    num = re.sub(r'2018-', '18', x)
    last = re.sub(r'-', '', num)
    all = []
    for i in range(8):  # --------------------------------七天之前
        cc = str(int(last) - i)
        all.append(cc)
    return all

def daterange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    return dates


def timeshow():
    all = []
    a = daterange("2018-04-25", "2018-04-28")
    for i in a:
        q = re.sub(r'2018-', '18', i)
        w = re.sub(r'-', '', q)
        all.append(w)
    return all




# def job2():
# san2 = bian()
san2 = timeshow()  # ~~~~~~~~~~~~~~~~可在上面固定日期
# san2=[180201.00000399]


xx = list(range(0, 1500))
xx.pop(0)
reall = []
for date in san2:
    date2 = int(date)
    # print(date2)
    time.sleep(1.5)
    rex = []
    for i in xx:
        re = date2 * 1e8 + i
        ree = int(re)
        rex.append(ree)
    reall.append(rex)
print(reall)

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                       connect_args={"charset": "utf8"}, echo=True, )
engine3 = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),
    connect_args={"charset": "utf8"}, echo=True, )
for ids in reall:
    break_count = 600  # 最大失败数量为10
    while break_count > 0:
        id = ids.pop(0)
        jid = jiami_all(id)
        print(jid)
        J_return = url_return(jid)
        time.sleep(1)
        if break_count == 0:
            break
        elif J_return == 'None':
            print(id)
            print("没有")
            break_count = break_count - 1

        else:
            a = random.randint(1, 10)  # 随机1-15的日期
            b = '1'
            RR = [jid, J_return, id, a, b]
            df = pd.DataFrame(RR)
            dataframe = df.T

            dataframe.columns = ["fund_id", "fund_name", "id_time", "priority", "is_used"]
            dataframe["source_id"] = '020002'
            print(dataframe)
            print(now2)
            # to_sql("jfz_id", engine, dataframe, type="update")
            to_sql("__id_search", engine3, dataframe, type="update")
            # print(jid)
            # jfz_id.append(jid)
            # jfz_name.append(J_return)
            # print(jid)
            # jfz_all.append(ALL)

print("DOWN")
