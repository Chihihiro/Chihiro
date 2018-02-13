# _*_ coding: utf-8 _*_
import requests
import re
import http.cookiejar
import urllib.request, urllib.parse, urllib.error
import http.cookiejar
import hashlib
from History.iosjk import to_sql
from History.engine import *
def crawl():
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

    pp = re.search(r'token\s+(.+?)\n.', data, re.DOTALL).group(1)
    token = str(pp)
    print(token)
    print('TOKEN')





# token采集结束-------------------------------------------------------------------------------------------------------
#
# fileobj = open('C:\\Users\\63220\\Desktop\\SMPP.txt','r')
# try:
#     strings = fileobj.read()
# finally:
#     fileobj.close()
# x= strings.split('\n')
# print(x)
def first():
    data = open('C:\\Users\\63220\\PycharmProjects\\QQX\\cookie_jar.txt').read()
    print(data)

    pp = re.search(r'token\s+(.+?)\n.', data, re.DOTALL).group(1)
    token = str(pp)
    print(token)
    print('TOKEN')

    df_limit=pd.read_sql("select fund_id,fund_name from zhaoyang WHERE version='{}' and source_id is  NULL ".format(now),engine5)
    x=to_list(df_limit)

    Result=[]

    for i in x:
        q=i[1]
        JR=i[0]
        url='https://www.12345fund.com/api/v1/dt_search/search_like?all_name={}&token={}'.format(q,token)
        print(url)
        r = requests.get(url).content.decode('utf8')
        print(r)
        if '成功' not in r:
            v='失败'
            Result.append(v)
            c=[now,v,JR]
            dff=pd.DataFrame(c)
            df=dff.T
            df.rename(columns={0:"version",1:"source_id",2:"fund_id"},inplace=True)
            print(df)
            to_sql("zhaoyang", engine5, df, type="update")

        else:
            R = re.search('"id":(.+?),', r, re.DOTALL).group(1)
            K = str(R)
            print(R)
            Result.append(K)
            c=[now,K,JR]
            dff=pd.DataFrame(c)
            df=dff.T

            df.rename(columns={0:"version",1:"source_id",2:"fund_id"},inplace=True)
            print(df)
            to_sql("zhaoyang", engine5, df, type="update")
    # d = pd.DataFrame(Result)
    # d.to_csv("C:\\Users\\63220\\Desktop\\ZYID1110.csv")
    print('done第一步')


# 第二部--------------------------------------------------------------------------------------------------
# from engine import *
# now = time.strftime("%Y-%m-%d")
#
# fileobj = open('C:\\Users\\63220\\Desktop\\ZYID3.txt','r')
# try:
#     strings = fileobj.read()
# finally:
#     fileobj.close()
# Result= strings.split('\n')
def second():
    data = open('C:\\Users\\63220\\PycharmProjects\\QQX\\cookie_jar.txt').read()
    print(data)

    pp = re.search(r'token\s+(.+?)\n.', data, re.DOTALL).group(1)
    token = str(pp)
    print(token)
    print('TOKEN')

    df_limit=pd.read_sql("select fund_id,source_id from zhaoyang WHERE version='{}' and source_id NOT like'失败' AND zhaoyang IS  NULL ".format(now),engine5)
    Result=to_list(df_limit)
    # result = []
    # name = []
    for i in Result:
        q=i[1]
        JR=i[0]
        url = 'https://www.12345fund.com/api/v1/fill_fund/get_basic_data?fund_id={}&token={}'.format(q, token)
        print(url)
        # r = requests.get(url).content.decode('utf8')
        try:
            r = requests.get(url).content.decode('utf8')
            n = re.search('"fund_name":"(.+?)"', r, re.DOTALL).group(1)
            K = str(n)
            print(K)
            day = re.search('"statistic_date":"(.+?) 00:00:00"', r, re.DOTALL).group(1)
            Day = str(day)
            c=[now,JR,Day]
            dff=pd.DataFrame(c)
            df=dff.T
            df.rename(columns={0:"version",1:"fund_id",2:"zhaoyang"},inplace=True)
            print(df)
            to_sql("zhaoyang", engine5, df, type="update")
        except BaseException:
            print("有错")
        else:
            pass


crawl()
first()
second()


print('done')




