# _*_ coding: utf-8 _*_
# from tieba import *
from engine import *

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


#
import time

now = time.strftime("%Y-%m-%d")


# def bian():
#     x = time.strftime("%Y-%m-%d")
#     num = re.sub(r'2018-', '18', x)
#     last= re.sub(r'-', '', num)
#     all = []
#     for i in range(8):#--------------------------------七天之前
#         cc = str(int(last) - i)
#         all.append(cc)
#     return all


def timeshow():
    all = []
    a = daterange("2018-03-24", "2018-03-26")
    for i in a:
        q = re.sub(r'2018-', '18', i)
        w = re.sub(r'-', '', q)
        all.append(w)
    return all


from job.tieba import *

san2 = timeshow()  # ~~~~~~~~~~~~~~~~可在上面固定日期


# san2=[180201.00000399]


def id_list():
    xx = list(range(1000))
    xx.pop(0)
    reall = []
    for date in san2:
        date2 = int(date)
        rex = []
        for i in xx:
            re = date2 * 1e8 + i
            ree = int(re)
            rex.append(ree)
        reall.append(rex)
    return reall


import random


def crawl(list):
    # for ids in list:
    break_count = 3000  # 最大失败数量为10
    while break_count > 0:
        id = list.pop(0)
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
            to_sql("__id_search", engine_crawl_private, dataframe, type="update")


from multiprocessing.dummy import Pool as ThreadPool

all = id_list()

pool = ThreadPool(4)
pool.map(crawl, all)
pool.close()
pool.join()
