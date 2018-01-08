#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
#
# LOGIN_URL = 'https://www.12345fund.com/api/v1/account/login_safe'
# values = {'account_name': 'E00002199','passwordmd5': '339377'} # , 'submit' : 'Login'
# postdata = urllib.parse.urlencode(values).encode()
# user_agent = r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
# headers = {'User-Agent': user_agent, 'Connection': 'keep-alive'}
#
#
# get_url = 'https://www.12345fund.com/html/fundDetail.html?fundId=107459'  # 利用cookie请求訪问还有一个网址




"""
__title__ = ''
__author__ = 'pi'
__mtime__ = '8/23/2015-023'
__email__ = 'pipisorry@126.com'
# code is far away from bugs with the god animal protecting
    I love animals. They taste delicious.
              ┏┓      ┏┓
            ┏┛┻━━━┛┻┓
            ┃      ☃      ┃
            ┃  ┳┛  ┗┳  ┃
            ┃      ┻      ┃
            ┗━┓      ┏━┛
                ┃      ┗━━━┓
                ┃  神兽保佑    ┣┓
                ┃　永无BUG。   ┏┛
                ┗┓┓┏━┳┓┏┛
                  ┃┫┫  ┃┫┫
                  ┗┻┛  ┗┻┛
"""
import urllib.request, urllib.parse, urllib.error
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

print ()