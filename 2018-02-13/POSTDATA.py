# -*- coding:utf-8 -*-
'''by sudo rm -rf  http://imchenkun.com'''
import scrapy
from faker import factory
from douban.items import DoubanMailItem
import urlparse

f = factory.create()


class MailSpider(scrapy.Spider):
    name = 'douban-mail'
    allowed_domains = ['accounts.douban.com', 'douban.com']
    start_urls = [
        'https://www.douban.com/'
    ]
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Connection': 'keep-alive',
        'Host': 'accounts.douban.com',
        'User-Agent': f.user_agent()
    }

    formdata = {
        'form_email': '您的账号',
        'form_password': '您的密码',
        # 'captcha-solution': '',
        # 'captcha-id': '',
        'login': '登录',
        'redir': 'https://www.douban.com/',
        'source': 'None'
    }

    def start_requests(self):
        return [scrapy.Request(url='https://www.douban.com/accounts/login',
                               headers=self.headers,
                               meta={'cookiejar': 1},
                               callback=self.parse_login)]