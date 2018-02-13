import requests
import re

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    # 'Content-Type': 'application/msexcel',
    'Host': 'www.bocichina.com',


}

def parse(response):

    block = re.search('<select name="productCode".+?</select>', response.body, re.DOTALL)
    print (response.headers)
    cookies =response.headers['Set-Cookie']
    cookies = cookies[0:cookies.index(';')].replace('JSESSIONID=','')
    print(cookies)
    if block:
        funds = re.findall(
            '<option value="(.+?)">(.+?)</option>', block.group())
        print(len(funds))
        for fund in funds[0:1]:
            #url = 'http://www.guodu.com/finance/' + fund[0]
            fund_id = fund[0]
            url = 'http://www.bocichina.com/boci/asset/mfinancing/productExcel.jsp?productCode=%s' % (fund_id,)
            print(url)
            fund_name = fund[1]
            response = requests.post(url, headers=headers, cookies={'JSESSIONID': cookies}, data={'productCode':fund_id})
            print (response.content)
            # soup = bs(response.content, 'lxml', from_encoding='utf8')
            # trs = soup.find('table').find_all('tr')
            # for tr in trs[0:100]:
            #     tds = tr.find_all('td')
            #     if len(tds) != 3 or '日期' in tds[0].text.strip():
            #         continue
            #     item = sFundNvDataItem()
            #     item['org_id'] = 'SG0041'
            #     item['fund_name'] = fund_name
            #     item['statistic_date'] = regularize_time(clean_str_strong(tds[0].text))
            #     item['source_code'] = 1
            #     item['nav'] = clean_str_strong(tds[1].text)
            #     item['added_nav'] = clean_str_strong(tds[2].text)
            #     #print item
            #     yield item

parse(response=requests.get('http://www.bocichina.com/boci/asset/mfinancing/productIntro.jsp?productCode=A80007'))