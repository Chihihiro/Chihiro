from engine import *
from multiprocessing.dummy import Pool as ThreadPool
import datetime

def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(7)
        date = dt.strftime("%Y-%m-%d")
    return dates

# a = datetime.timezone(1)
co=0

a=dateRange('2017-08-02',now_time(-7-co))
b=dateRange('2017-08-08',now_time(-co))
all=[]
for num in range(len(a)):
    i=a[num]
    o=b[num]
    vv=[i,o]
    all.append(vv)

a1=dateRange('2016-08-02',now_time(-7-co))
b1=dateRange('2016-08-08',now_time(co))
all1=[]
for num in range(len(a1)):
    i=a1[num]
    o=b1[num]
    vv=[i,o]
    all1.append(vv)


a2=dateRange('2015-08-02',now_time(-7-co))
b2=dateRange('2015-08-08',now_time(--co))
all2=[]
for num in range(len(a2)):
    i=a2[num]
    o=b2[num]
    vv=[i,o]
    all2.append(vv)

def readsql(list):
    all=[]
    i=list[0]
    o=list[1]
    df = pd.read_sql("SELECT COUNT(fund_id) FROM fund_nv_data_source_copy2 WHERE source_id='020003' and statistic_date BETWEEN '{}' AND '{}'".format(i, o), engine_base)
    b = df.iloc[0, 0]
    c=[i,b]
    all.append(c)
    return all

pool = ThreadPool(6)
ALL=[]
ALL.append(pool.map(readsql,all))
list1=[]
for i in ALL[0]:
    print(i[0])
    list1.append(i[0])
print(list1)




# def readsql_copy2(list):
#     all=[]
#     i=list[0]
#     o=list[1]
#     df = pd.read_sql("SELECT COUNT(fund_id) FROM fund_nv_data_standard_copy2 WHERE statistic_date BETWEEN '{}' AND '{}'".format(i, o), engine_base)
#     b = df.iloc[0, 0]
#     c=[i,b]
#     all.append(c)
#     return all

pool = ThreadPool(6)
ALL_copy2=[]
ALL_copy2.append(pool.map(readsql,all1))
list_copy2=[]
for i in ALL_copy2[0]:
    print(i[0])
    list_copy2.append(i[0])

print(list_copy2)






pool = ThreadPool(6)
ALL_2015=[]
ALL_2015.append(pool.map(readsql,all2))
list_2015=[]
for i in ALL_2015[0]:
    print(i[0])
    list_2015.append(i[0])

print(list_2015)



