# # -*- coding: utf-8 -*-
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
#
# """
# 基协
# """
# import scrapy
# import re
# from bs4 import BeautifulSoup
# from fund_spider.items import xFundOrgItem, xFundOrgSRItem, xFundExecPersonItem
# import requests,json,random
# from util.str_clean import clean_str_strong
# class TrustSxxtSpider(scrapy.Spider):
#     name = "jixie_fund_org_spider"
#     allowed_domains = ["http://gs.amac.org.cn/"]
#
#     custom_settings = {
#         'DOWNLOAD_DELAY':1.2,
#         'CONCURRENT_REQUESTS_PER_IP': 16,
#                     }
#
#     headers = {
#         'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#         'Accept-Encoding':'gzip, deflate, sdch',
#         'Accept-Language':'zh-CN,zh;q=0.8',
#         'Cache-Control':'max-age=0',
#         'Connection':'keep-alive',
#         'Cookie':'look=first',
#         'Host':'gs.amac.org.cn',
#         'If-Modified-Since':'Sun, 02 Jul 2017 21:29:20 GMT',
#         'If-None-Match':'',
#         'Upgrade-Insecure-Requests':'1',
#         'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
#     }
#
#     def start_requests(self):
#         url = 'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/managerList.html'
#         r = requests.get(url, data='{}', headers={'If-None-Match':''})
#         controls = re.findall('onmouseover="javascript:chooseFundType\(this,\'showFundScale.+?</div>', r.content, re.DOTALL)
#         for c in controls[0:]:
#             orgtype = re.search('value="(.+?)"', c).group(1)
#             ranges = re.findall('<li onclick="javascript:chooseFundScale\(this,\'(\d+?)\',\'(\d*?)\'\);">(.+?)</li>', c)
#             for range in ranges[0:]:
#                 if range[1]:
#                     data = {'fundScale': {'to': "%s" %(range[1],) ,'from': "%s" %(range[0],) }, 'fundType': "%s" %(orgtype, )}
#                 else:
#                     data = {'fundScale': {'from': "%s" % (range[0],)},'fundType': "%s" % (orgtype,)}
#                 print data
#                 url = 'http://gs.amac.org.cn/amac-infodisc/api/pof/manager?rand=%s&page=0&size=100' %random.random()
#                 yield scrapy.Request(url, method='POST', body=json.dumps(data), headers={'Content-Type':'application/json'}, meta={'asset_mgt_scale_range':range[2], 'orgtype':orgtype, 'data':data}
#                                      ,callback=self.parse)
#
#
#     def parse(self, response):
#         page = json.loads(response.body)['totalPages']
#         print '共 %d 页' %page
#         # for i in range(0,-1,-1):
#         for i in range(page, -1, -1):
#             self.log('crawling page:%d' %i)
#             url = 'http://gs.amac.org.cn/amac-infodisc/api/pof/manager?rand={}&page={}&size=100'.format(random.random(),i)
#             yield scrapy.Request(url, method='POST', body=json.dumps(response.meta['data']), headers={'Content-Type': 'application/json'},
#                              meta=response.meta
#                              , callback=self.parse_page,  dont_filter=True)
#
#     def parse_page(self, response):
#         for org in json.loads(response.body)['content']:
#             org_url = org['url']
#             url = 'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/'+org_url
#             yield scrapy.Request(url,callback=self.parse_, dont_filter=True,  meta=response.meta)
#
#         #yield scrapy.Request(url, body='{}',method="POST",headers=headers,callback=self.parse_) 这样获取不到，待仔细研究
#
#         # ##测试用
#         # url='http://gs.amac.org.cn/amac-infodisc/res/pof/manager/1702071153102357.html'
#         # yield scrapy.Request(url, callback=self.parse_, headers=self.headers)
#
#
#
#     def parse_(self, response):
#         self.log(response.url)
#         soup = BeautifulSoup(response.body, "lxml")
#         infos = soup.find("table", {"class": "table table-center table-info"})
#         # print xx
#         # return
#         #infos = response.xpath('//table[@class="table table-center table-info"]//tr')
#
#         item=xFundOrgItem()
#         integrity_info = infos.find('td', text=u'机构诚信信息:').find_next_sibling('td').find_all('td')  # 机构诚信信息
#         item['integrity_info'] = [i.find('span') for i in integrity_info]
#         item['integrity_info'] = [i.text if i else '' for i in item['integrity_info']]
#         integrity_info = [i.text.replace(i.find('span').text, '') if i.find('span') else i.text for i in integrity_info]
#         item['integrity_info'] = zip(item['integrity_info'], integrity_info)
#         item['integrity_info'] = ';'.join([':'.join([j for j in i if j]) for i in item['integrity_info']])
#
#         #会员信息
#         item['is_member'] = infos.find('td', text=u'是否为会员:')
#         item['member_type'] = infos.find('td', text=u'当前会员类型:')
#         item['initiation_time'] = infos.find('td', text=u'入会时间:')
#         if item['is_member']:
#             item['is_member']=item['is_member'].find_next_sibling('td').text
#         if item['member_type']:
#             item['member_type'] = item['member_type'].find_next_sibling('td').text
#             item['initiation_time'] = item['initiation_time'].find_next_sibling('td').text
#
#         item['Legal_opinion_status'] = infos.find('td', text=u'法律意见书状态:')
#         item['law_firm_name'] = infos.find('td', text=u'律师事务所名称:')
#         item['lawyer_name'] = infos.find('td', text=u'律师姓名:')
#         if item['Legal_opinion_status']:
#             item['Legal_opinion_status'] = item['Legal_opinion_status'].find_next_sibling('td').text
#         if item['law_firm_name']:
#             item['law_firm_name'] = item['law_firm_name'].find_next_sibling('td').text
#             item['lawyer_name'] = item['lawyer_name'].find_next_sibling('td').text
#
#
#         item['org_id'] = infos.find('td',text=u"登记编号:").find_next_sibling('td').text  # 机构ID,
#         #item['org_name'] = ''  #
#         item['org_name_en'] = infos.find('td',text=u"基金管理人全称(英文):").find_next_sibling('td').text  # 机构英文,
#         #item['org_name_py'] = ''  # 机构简拼,
#         item['org_full_name'] = infos.find('div',id="complaint1").text.strip().replace('&nbsp','').replace('\t','') # 机构全名,
#         item['found_date'] = infos.find('td',text=u"成立时间:").find_next_sibling('td').text   # 机构成立日期,
#         item['org_code'] = infos.find('td',text=u"组织机构代码:").find_next_sibling('td').text # 组织机构代码,
#         item['reg_code'] = item['org_id']  # 登记编号,
#         item['reg_time'] = infos.find('td',text=u"登记时间:").find_next_sibling('td').text  # 登记时间,
#         item['manage_type'] =  infos.find('td',text=u"业务类型:").find_next_sibling('td').text  # 管理基金主要类型,
#         #item['other_manage_type'] =  infos.find('td',text=u"申请的其他业务类型:").find_next_sibling('td').text  # 申请的其他业务类型,
#         #item['manage_fund_id'] = ''  # 管理的基金ID,
#
#         manage_fund1 = infos.find('td',text=u"暂行办法实施前成立的基金:").find_next_sibling('td').find_all('a')
#         manage_fund2 = infos.find('td', text=u'暂行办法实施后成立的基金:').find_next_sibling('td').find_all('a')
#         if not manage_fund1:
#             manage_fund1=''
#         if not manage_fund2:
#             manage_fund2=''
#         item['manage_fund'] = '暂行办法实施前成立的基金:'+' '.join([i.text.strip() for i in manage_fund1])+ \
#                               '暂行办法实施后成立的基金:'+' '.join([i.text.strip() for i in manage_fund2])# text COMMENT 管理的基金,
#
#         #item['researcher_scale'] = scrapy.Field()  #  投研人员规模,
#         #item['asset_mgt_scale'] = scrapy.Field()  # 资产管理规模(亿元),
#         #item['asset_mgt_scale_range'] = scrapy.Field()  #  资产管理规模范围(亿元),
#
#
#         item['property'] = infos.find('td',text=u"企业性质:").find_next_sibling('td').text  # 企业性质,
#         item['reg_capital'] = infos.find('td',text=u"注册资本(万元)(人民币):")
#         if item['reg_capital']:
#             item['currency']='CNY'
#             item['reg_capital']=item['reg_capital'].find_next_sibling('td').text.strip().replace(',','').replace('，','')# 注册资本,
#             item['real_capital'] = infos.find('td',text=u"实缴资本(万元)(人民币):").find_next_sibling('td').text.strip().replace(',','').replace('，','')  #  实缴资本,
#         else:
#             item['currency'] = 'USD'
#             item['reg_capital'] = infos.find('td', text=u"注册资本(万元)(美元):").find_next_sibling('td').text
#             item['real_capital'] = infos.find('td', text=u"实缴资本(万元)(美元):").find_next_sibling('td').text
#         item['real_capital_proportion'] = infos.find('td',text=u"注册资本实缴比例:").find_next_sibling('td').text  # 注册资本实缴比例,
#
#
#         item['legal_person'] =  infos.find('td',text=u'法定代表人/执行事务合伙人(委派代表)姓名:').find_next_sibling('td').text  # 法人代表,
#         item['legal_person_resume'] = infos.find('table',{'class':'table table-noborder table-responsive'}).text.replace('时间：',' 时间：').replace('任职单位：', ' 任职单位：').replace('职务：',' 职务:')   # 法人代表工作履历,
#         item['is_qualified'] =  infos.find('td',text=u'是否有从业资格:').find_next_sibling('td').text  # 法人是否有从业资格,
#         item['qualifying_way'] =  infos.find('td',text=u'资格取得方式:').find_next_sibling('td').text  # 法人从业资格获得方式,
#         #item['country'] =   # 国家,
#         #item['prov'] =   # 省/直辖市,
#         #item['city'] =   # 市,
#         #item['area'] =  # 区,
#         item['address'] = infos.find('td',text=u"办公地址:").find_next_sibling('td').text  # 联系地址,
#         item['reg_address'] = infos.find('td',text=u"注册地址:").find_next_sibling('td').text  # 注册地址,
#         item['org_web'] = infos.find('td',text=u"机构网址:").find_next_sibling('td').text  # 公司网站,
#         item['employeescale'] = infos.find('td',text=u"员工人数:").find_next_sibling('td').text  # 员工人数,
#         #item['abnormal_liquidation_fund'] =   # 非正常清盘基金,
#         item['final_report_time'] = infos.find('td',text=u'机构信息最后更新时间:').find_next_sibling('td').text  # 机构信息最后报告时间,
#         item['special_tips'] = infos.find('td',text=u'特别提示信息:').find_next_sibling('td').text  # 特别提示信息
#
#         #print item
#         for i in item:
#             if "name" not in i:
#                 item[i] = clean_str_strong(item[i])
#             else:
#                 item[i] = clean_str_strong(item[i],'edge')
#         item['org_category'] = 4  # 机构类别,
#         yield item
#
#         tds = soup.find_all('table', {'class': 'table table-noborder table-responsive'})[1].find_all('td')
#         for i, td in enumerate(tds):
#             if '高管姓名：' in td.text or '高管姓名:' in td.text:
#                 try:
#                     epitem = xFundExecPersonItem()
#                     epitem['org_id'] = item['org_id']
#                     epitem['reg_code'] = item['reg_code']
#                     epitem['org_full_name'] = item['org_full_name']
#                     epitem['name'] = re.search(u'姓名[：:](.+)', td.text).group(1).strip()
#                     epitem['duty'] = ','.join(re.findall('\S+', tds[i + 1].text.replace('职务：', '').replace('职务:', '')))
#                     epitem['is_qualified'] = '是' if '是' in tds[i + 2].text.replace('是否具有基金从业资格','') else '否'
#                     epitem['qualifying_way'] = tds[i + 2].text.split('是')[-1].replace('(', '').replace(')', '').strip() if \
#                                         epitem['is_qualified'] == '是' else None
#                 except Exception:
#                     pass
#                 else:
#                     print epitem
#                     yield epitem
#
#         itemSR = xFundOrgSRItem()
#         itemSR['org_id'] = item['org_id']
#         itemSR['org_full_name'] = item['org_full_name']
#         itemSR['org_scale_type'] = response.meta['orgtype']
#         itemSR['org_scale_range']= response.meta['asset_mgt_scale_range']
#         itemSR['org_tips']= item['special_tips']
#         itemSR['integrity_information']=clean_str_strong(infos.find('td',text=u"机构诚信信息:").find_next_sibling('td').text)
#         itemSR['legal_person']= item['legal_person']
#         itemSR['reg_place']=item['reg_address']
#         itemSR['reg_code']=itemSR['org_id']
#         itemSR['found_date']=item['found_date']
#         itemSR['reg_time']=item['reg_time']
#
#         yield itemSR
#
