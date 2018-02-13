# _*_ coding: utf-8 _*_
import pandas as pd
import time
import requests
import re



fileobj = open('C:\\Users\\63220\\Desktop\\SMPP.txt','r')
try:
    strings = fileobj.read()
finally:
    fileobj.close()
x= strings.split('\n')
print(x)


Result=[]

for q in x:

    url='http://serve.simuwang.com/index.php?m=Search&c=search&a=quickSearch&count=10&callback=jQuery17205335830923285313_1502336860069&search_type=all&skeyword={}&_=1502336885161'.format(q)
    # q = 'http://www.jinfuzi.com/product/{}.html'.format(q)
    print(url)
    r = requests.get(url).content.decode('utf8')
    print(r)
    if 'id' not in r:
        v='None'
        Result.append(v)
        f = open("C:\\Users\\63220\\Desktop\\SMPPID922.txt", "a+")
        f.write( v )
        f.write("\n")
        f.close()
    else:
        R = re.search('"data_id":"(.+?)","name"', r, re.DOTALL).group(1)
        K = str(R)
        print(R)
        Result.append(K)
        f = open("C:\\Users\\63220\\Desktop\\SMPPID922.txt", "a+")
        f.write(K )
        f.write("\n")
        f.close()


print(Result)
# Result.pop(-1)
d = pd.DataFrame(Result)
d.to_csv("C:\\Users\\63220\\Desktop\\SMPPID922.txt")
print('done第一步')


#########第二部
# fileobj = open('C:\\Users\\63220\\Desktop\\SMPP222.txt','r')
# try:
#     strings = fileobj.read()
# finally:
#     fileobj.close()
# Result= strings.split('\n')



headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
cookie = []
result=[]
name = []
for q in Result:

    url='http://dc.simuwang.com/product/{}.html'.format(q)
    print(url)
    r = requests.get(url,headers=headers).content.decode('utf8')
    # time.sleep(1)
    # print(r)
    t='<div class="deadLine" style="left: 90px;top: 8px;">(截止日期'
    if t not in r:
        v='None'
        result.append(v)
        name.append(v)
        # f = open("C:\\Users\\63220\\Desktop\\SMPPDATE922.txt", "a+")
        # f.write(v)
        # f.write("\n")
        # f.close()
    else:
        R = re.search('<div class="deadLine".*?截止日期：(.+?)\)</div>', r, re.DOTALL).group(1)
        K = str(R)
        print(R)
        T = '-'
        if T not in K:
            V='None'
            result.append(V)
        else:
            result.append(K)
            n = re.search('<span class="h3 blackTxt txtBold fl fname">(.+?)</span>', r, re.DOTALL).group(1)
            print(n)
            N = str(n)
            name.append(N)
            time.sleep(2.5)

        # time.sleep(1)


uu=[name,result]
d = pd.DataFrame(uu).T
d.to_csv("C:\\Users\\63220\\Desktop\\SMPP922.csv")
print('done')