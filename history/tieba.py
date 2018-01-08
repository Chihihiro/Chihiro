# _*_ coding: utf-8 _*_
import http.cookiejar
import urllib.request, urllib.parse, urllib.error
import http.cookiejar
import re
import time


import urllib.request, urllib.parse, urllib.error
import http.cookiejar
import hashlib
import time
import requests
import re
from urllib import request
from http import cookiejar
import http.cookiejar
import urllib.request, urllib.parse, urllib.error
import http.cookiejar
import hashlib
import datetime
import pandas as pd
import xlrd
import sys
from iosjk import to_sql
from sqlalchemy import create_engine
import socket
login_url = 'https://passport.jinfuzi.com/passport/user/login'
# post_URL = 'http://www.shaidanwang.cn/alertLogin.do?shoveDate{}'.format(str(time.time())[0:10])
post_URL = 'https://passport.jinfuzi.com/passport/user/doLogin.html'
print(post_URL)
get_url = 'https://www.jinfuzi.com/simu/p-P61l7o8edk.html'  # 利用cookie请求訪问还有一个网址

values = {'paramMap.password':'68125542','paramMap.userName':'15026588463'}

postdata = urllib.parse.urlencode(values).encode()
user_agent = r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
headers = {'User-Agent': user_agent}

cookie_filename = 'cookie_jar.txt'
cookie_jar = http.cookiejar.MozillaCookieJar(cookie_filename)
handler = urllib.request.HTTPCookieProcessor(cookie_jar)
opener = urllib.request.build_opener(handler)

request = urllib.request.Request(login_url, headers=headers)
print(request)
try:
    print ('-------------')
    response = opener.open(request)
    request = urllib.request.Request(post_URL, data=postdata, headers=headers,method='POST')
    response = opener.open(request)
    # print(response.read().decode())
    # ll=re.search('"token":"(.+?)"',response,re.DOTALL).group(1)
    # print(ll)
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

# exit()

#
# def login(self):
#     session = requests.Session()
#     url = 'https://passport.jinfuzi.com/passport/user/doLogin.html'
#     session.headers[
#         'User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
#
#     # 金斧子账号  备用：18939862542 pwd： 68125542
#     data = {"LoginForm[username]": "15026588463", "LoginForm[password]": "68125542"}
#     headers = session.post(url,
#                            data=data,
#                            allow_redirects=False).headers['Location']
#     if 'jfz' not in headers:
#         time.sleep(10)
#         headers = session.post(url,
#                                data=data,
#                                allow_redirects=False).headers['Location']
#     headers = session.get(headers.replace('http:', 'https:'), allow_redirects=False).headers
#     self.cookies = {'PHPSESSID': re.search('PHPSESSID=(.+?);', str(headers)).group(1), 'isChecked': 'true'}
#     self.log('%s获取到的session为： %s' % (datetime.now(), self.cookies['PHPSESSID']))  # 过期：1800s


def url_return(id):
    get_url ='https://www.jinfuzi.com/simu/p-{}.html'.format(id)
    # get_url = 'https://www.jinfuzi.com/product/{}.html'.format(id)  # 利用cookie请求訪问还有一个网址
    user_agent = r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
    headers = {'User-Agent': user_agent}

    cookie_filename = 'cookie_jar.txt'
    cookie_jar = http.cookiejar.MozillaCookieJar(cookie_filename)
    handler = urllib.request.HTTPCookieProcessor(cookie_jar)
    opener = urllib.request.build_opener(handler)
    # time.sleep(0.5)


    # get_request = urllib.request.Request(get_url, headers=headers)
    # get_response = opener.open(get_request)
    # xx =get_response.read().decode()
    # print(xx)
    # cc='<span class="crumb crumb-end">'
    # if cc in xx:
    try:
        # s = requests.Session()

        timeout = 10

        socket.setdefaulttimeout(timeout)

        get_request = urllib.request.Request(get_url, headers=headers)
        get_response = opener.open(get_request)
        xx = get_response.read().decode()
        R = re.search("""<span class="crumb crumb-end">(.+?)</span>""", xx, re.DOTALL).group(1)
        # return R
    except BaseException:
        R='None'
        return  R
    else:
        return R
    # else:
    #     R='None'
    #     return R

#
id='P61yt7b1mp'
xx=url_return(id)
print(xx)
# id2='p-P61yt7b1mp'
# yy=url_return(id2)
# print(yy)



# R = re.search("""<div class="entity_wrap">.*?<div class="title" title="(.+?)">""", xx, re.DOTALL).group(1)


print("tieba.py 启用")

