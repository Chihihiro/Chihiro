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


from requests.auth import AuthBase
# from requests.auth import HTTPBasicAuth
# import requests
#
# auth = HTTPBasicAuth('15026588463','68125542')
# r =requests.post(url="https://www.jinfuzi.com/",auth=auth)
#
# print(r.text)
#
# exit()


def login():
    session = requests.Session()
    url = 'https://passport.jinfuzi.com/passport/user/doLogin.html'
    session.headers[
        'User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'

    # 金斧子账号  备用：18939862542 pwd： 68125542
    data = {"LoginForm[username]": "15026588463", "LoginForm[password]": "68125542"}
    headers = session.post(url,
                           data=data,
                           allow_redirects=False).headers['Location']
    if 'jfz' not in headers:
        time.sleep(10)
        headers = session.post(url,
                               data=data,
                               allow_redirects=False).headers['Location']
    headers = session.get(headers.replace('http:', 'https:'), allow_redirects=False).headers
    cookies = {'PHPSESSID': re.search('PHPSESSID=(.+?);', str(headers)).group(1), 'isChecked': 'true'}
    print (cookies)
    # log('%s获取到的session为： %s' % (datetime.now(), self.cookies['PHPSESSID']))  # 过期：1800s




# def url_return(id):
#     get_url = 'https://www.jinfuzi.com/product/{}.html'.format(id)  # 利用cookie请求訪问还有一个网址
#     user_agent = r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
#     headers = {'User-Agent': user_agent}
#
#     cookie_filename = 'cookie_jar.txt'
#     cookie_jar = http.cookiejar.MozillaCookieJar(cookie_filename)
#     handler = urllib.request.HTTPCookieProcessor(cookie_jar)
#     opener = urllib.request.build_opener(handler)
#     time.sleep(0.5)
#
#
#     get_request = urllib.request.Request(get_url, headers=headers)
#     get_response = opener.open(get_request)
#     xx =get_response.read().decode()
#     print(xx)
#     cuo='<form action="/search/index/simuindex.html" target="_blank" class="search-form">'
#     if cuo in xx:
#         R='None'
#         return R
#     else:
#         try:
#             R = re.search("""<div class="entity_wrap">.*?<div class="title" title="(.+?)">""", xx, re.DOTALL).group(1)
#             return R
#         except :
#             pass
#
#
# #
# id='05P61yt7b1mp'
# xx=url_return(id)
# print(xx)
# R = re.search("""<div class="entity_wrap">.*?<div class="title" title="(.+?)">""", xx, re.DOTALL).group(1)


# print("tieba.py 启用")
if __name__=='__main__':
         login()
