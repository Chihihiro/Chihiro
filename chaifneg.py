import pandas as pd
import time
import requests
import re
from bs4 import BeautifulSoup
import pymysql
import requests
import re
import os
fileobj = open('C:\\Users\\63220\\Desktop\\jinfuzi.txt','r')
try:
    strings = fileobj.read()
finally:
    fileobj.close()
x= strings.split('\n')
print(x)
print(len(x))
names=[]
guanlirens=[]
ids=[]
tougus=[]
for i in x:
    name = re.search('基金名称:(.+?),', i, re.DOTALL).group(1)
    # name = str(name)
    print(name)
    names.append(name)

    # print(i)
    guanliren = re.search('管理人:(.+?),', i, re.DOTALL).group(1)
    guanlirens.append(guanliren)
    id = re.search('ID:(.+?),', i, re.DOTALL).group(1)
    ids.append(id)
    tougu = re.search('投顾:(.+?),', i, re.DOTALL).group(1)
    tougus.append(tougu)
R = [ids,names,tougus,guanlirens]
d = pd.DataFrame(R).T
d.columns = ["ID","name","投资顾问", "管理人"]
d.to_csv("C:\\Users\\63220\\Desktop\\JFZBC.csv")
print('done第一步')










connect = pymysql.connect(  # 连接数据库服务器
    user="root",
    password="",
    host="localhost",
    port=3306,
    db="test",
    charset="utf8"
)
con = connect.cursor()  # 设置游标



f = open("C:\\Users\\63220\\Desktop\\jinfuzi.txt","r")
while True:
    line = f.readline()

    # x = line.re.sub('基金管理人:', '')
    line=re.sub('基金名称:', '',line)
    line = re.sub('ID:', '',line)
    line = re.sub('投顾:', '',line)
    line = re.sub('管理人:', '',line)
    print(line)


    if line:
        line = line.strip("\n")
        line = line.split(",")  # 你写如.txt文件的数据用逗号分开，此时用逗号将他们转化为列表
        print(line)
        uuname = line[0]
        print(uuname)
        fund_mananger = line[1]
        fund_mananger_nominal = line[2]
        id = line[3]
        # id = re.search('ID:(.+?),', id1, re.DOTALL).group(1)
        # id = str(id)
        con.execute("insert into jinfuzi(id,uuname,fund_manager,fund_manager_nominal)values(%s,%s,%s,%s)",
                    [id,uuname,fund_mananger,fund_mananger_nominal])
    else:  # 导入数据库
        break
connect.commit()  # 这句记得写上提交数据，否则导入为空(有的DDL是不需要导入的)
con.close()  # 最后记得关掉连接
connect.close()
print('down')


