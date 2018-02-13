# _*_ coding: utf-8 _*_
import pandas as pd
import time
import requests
from random import choice
from urllib.error import URLError, HTTPError
import re
from http import cookiejar
import http.cookiejar
import urllib.request as rq, urllib.parse, urllib.error
import http.cookiejar
import hashlib
login_url = 'https://www.12345fund.com/html/login.html'
post_URL = 'https://www.12345fund.com/api/v1/account/login_safe'
get_url = 'https://www.12345fund.com/html/fundDetail.html?fundId=102015'  # 利用cookie请求訪问还有一个网址

hash = hashlib.md5()
hash.update('339377'.encode('utf-8'))
hs1 = hash.hexdigest()
hash = hashlib.sha1()
hash.update(hs1.encode('utf8'))
passw = hash.hexdigest()
print(passw)

values = {'account_name': 'E00002199',
          'autologin':'1',
          'passwordmd5': passw}


postdata = urllib.parse.urlencode(values).encode()
user_agent = r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
headers = {'User-Agent': user_agent}

cookie_filename = 'cookie_jar.txt'
cookie_jar = http.cookiejar.MozillaCookieJar(cookie_filename)
handler = urllib.request.HTTPCookieProcessor(cookie_jar)
opener = urllib.request.build_opener(handler)

request = urllib.request.Request(login_url, headers=headers)
try:
    print ('-------------')
    response = opener.open(request)
    request = urllib.request.Request(post_URL, data=postdata, headers=headers,method='POST')
    response = opener.open(request)
    print(response.read().decode())
except urllib.error.URLError as e:
    print(e.code, ':', e.reason)

cookie_jar.save(ignore_discard=True, ignore_expires=True)  # 保存cookie到cookie.txt中
for item in cookie_jar:
    print('Name = ' + item.name)
    print('Value = ' + item.value)

get_request = urllib.request.Request(get_url, headers=headers)
get_response = opener.open(get_request)
print(get_response.read().decode())

print ('cookies 采集')



data = open('C:\\Users\\63220\\PycharmProjects\\QQX\\cookie_jar.txt').read()
print(data)

pp = re.search(r'token\s+(.+?)\n.',data,re.DOTALL).group(1)
token = str(pp)
print(token)
print ('TOKEN')
# exit()

# token采集结束-------------------------------------------------------------------------------------------------------

fileobj = open('C:\\Users\\63220\\Desktop\\org918.txt','r')
try:
    strings = fileobj.read()
finally:
    fileobj.close()
x= strings.split('\n')
print(x)
#
#
# Result=[]
#
# for q in x:
#     # url='http://www.whatismyip.com.tw/'
#     url='https://www.12345fund.com/api/v1/dt_search/search_like?all_name={}&token={}'.format(q,token)
#     # print(url)
#
#     proxy_list = [{'https': '180.169.5.126'},{'http': '180.169.5.126'}]
#     while True:
#         try:
#             proxy = choice(proxy_list)
#             # for proxy in proxy_list:
#             print(proxy)
#
#             proxy_support = rq.ProxyHandler(proxy)
#             opener = urllib.request.build_opener(proxy_support)
#             opener.addheaders = [('User-Agent',
#                                   'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')]
#             urllib.request.install_opener(opener)
#             response = urllib.request.urlopen(url,timeout = 2)
#             html = response.read().decode("utf-8")
#             r = html
#             print(r)
#             print(proxy, '正确')
#             break
#         except urllib.error.URLError as e :
#             print(proxy,'错误')
#
#
#
#
#
#
#
#
#     #
#     # html = response.read().decode("utf-8")
#     # print(html)
#     # r=html
#     print('down')
#     exit()
#
#
#
#     # r = requests.get(url,headers=headers).content.decode('utf8')
#     time.sleep(1.5)
#     # print(r)
#     if '成功' not in r:
#         v='None'
#         Result.append(v)
#         f = open("C:\\Users\\63220\\Desktop\\org写入id测试919.txt", "a+")
#         f.write('朝阳ID:' + v + ',')
#         f.write("\n")
#         f.close()
#     else:
#         R = re.search('"id":(.+?),', r, re.DOTALL).group(1)
#         K = str(R)
#         print(R)
#         Result.append(K)
#         f = open("C:\\Users\\63220\\Desktop\\org写入id测试919.txt", "a+")
#         f.write('朝阳ID:' + K+ ',')
#         f.write("\n")
#         f.close()
#
#
# print(Result)
# # Result.pop(-1)
# d = pd.DataFrame(Result)
# d.to_csv("C:\\Users\\63220\\Desktop\\orgid919.csv")
# print('done第一步')


# 第二部----------------------------------------------------



fileobj = open('C:\\Users\\63220\\Desktop\\zyorgid921.txt','r')
try:
    strings = fileobj.read()
finally:
    fileobj.close()
Result= strings.split('\n')


iid =[]
name = []
pid = []
web = []
for q in Result:
    if q=='None':
        v = 'None'
        name.append(v)
        pid.append(v)
        web.append(v)
    else:
        # url='https://www.12345fund.com/api/v1/fill_org/get_org_data?fund_manager_id=21742&token=a9e8ad1b240d42759fde908ab4ccf6ec'
        # url1 = 'https://https://www.12345fund.com/api/v1/fill_org/get_org_data?fund_manager_id={}&token={}'.format(q, token)
        # headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0',
        #          'Referer':url1,
        #          'Cookie':'web=a9e8ad1b240d42759fde908ab4ccf6ec1505182624256; name=E00002199; flag=a9e8ad1b240d42759fde908ab4ccf6ec; isfill=0; father=-1; nick_name=E00002199; token=a9e8ad1b240d42759fde908ab4ccf6ec; session=a9e8ad1b240d42759fde908ab4ccf6ec1505182624256; pm="[\"sm3_0307\",\"sm3_0309\",\"sm3_0201\",\"sm3_0202\",\"sm3_0604\",\"G2_033\",\"sm3_0307_03\",\"sm3_0101\",\"sm3_0307_01\",\"sm3_0102\",\"sm3_0307_02\",\"sm3_0601\",\"sm3_0602\",\"sm3_0603\",\"sm3_0402\",\"sm3_0302\",\"sm3_0301\",\"sm3_0303\",\"sm3_0401\",\"sm3_08\",\"sm3_02\",\"sm3_03\",\"sm3_04\",\"sm3_05\",\"sm3_01\",\"sm3_05_02\",\"sm3_05_03\",\"sm3_05_01\"]"; goods_version=; is_index=0'}
        url ='https://www.12345fund.com/api/v1/fill_org/get_org_data?fund_manager_id={}&token={}'.format(q,token)
        print(url)
        r = requests.get(url,headers=headers).content.decode('utf8')
        print(r)
        n = re.search('"org_full_name":"(.+?)"', r, re.DOTALL).group(1)
        K = str(n)
        name.append(K)
        print(K)
        Pid = re.search('"org_web":(.+?)"', r, re.DOTALL).group(1)
        D = str(Pid)
        pid.append(D)
        w = re.search('"profile":(.+?),"tags"', r, re.DOTALL).group(1)
        W = str(w)
        web.append(W)
        iid.append(q)
        f = open("C:\\Users\\63220\\Desktop\\org写入921.txt", "a+")
        f.write('朝阳ID:' + q + '！！！')
        f.write('基金名称:' + K + '！！！')
        DD=D.replace("\n", "")
        f.write('网站:' + DD + '！！！')
        f.write('简介:' + W + '！！！')
        f.write("\n")
        f.close()
        time.sleep(2.1)




uu=[iid,name,pid,web]
d = pd.DataFrame(uu).T
d.to_csv("C:\\Users\\63220\\Desktop\\org919.csv")
print('done')