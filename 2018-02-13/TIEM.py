# -*- coding: UTF-8 -*-
from urllib import request
# if __name__ == "__main__":
#     url = "https://www.baidu.com/"
#     # 这是代理IP
#     proxy = {'http': '101.37.79.125'}
#     # 创建ProxyHandler
#     proxy_handler = request.ProxyHandler(proxy)
#     # 创建opener
#     opener = request.build_opener(proxy_handler)
#     # 添加User agent
#     opener.addheaders = [('User-Agent',
#                           'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'),
#                          ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
#                          ('Referer', 'http://www.ip138.com/'),
#                          ('Accept-Language', 'zh-CN,zh;q=0.8'),
#                          ('Accept-Encoding', 'gzip,deflate, sdch'),
#                          ('Host', '1212.ip138.com')]


# if __name__ == "__main__":
#     #访问网址
#     url = "https://www.baidu.com/"
#     #这是代理IP
#     proxy = {'http': '101.37.79.125'}
#     #创建ProxyHandler
#     proxy_support = request.ProxyHandler(proxy)
#     #创建Opener
#     opener = request.build_opener(proxy_support)
#     #添加User Angent
#     opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')]
#     #安装OPener
#     request.install_opener(opener)
#     #使用自己安装好的Opener
#     response = request.urlopen(url)
#     #读取相应信息并解码
#     html = response.read().decode("utf-8")
#     #打印信息
#     print(html)
from random import choice
proxy_list = [{'http': '101.37.79.125'},{'http': '120.77.210.59'},	{'http': '175.155.24.8'},	{'http': '218.75.144.25'},	{'http': '46.191.239.111'},	{'http': '61.178.238.122'},	{'http': '178.33.62.155'},	{'http': '188.38.105.21'},	{'http': '138.117.143.226'},	{'http': '181.39.223.96'},	{'http': '177.66.245.186'},	{'http': '180.76.134.106'},	{'http': '51.15.41.74'},	{'http': '61.143.228.162'},	{'http': '183.45.88.6'},	{'http': '186.10.5.142'},	{'http': '119.254.102.90'},	{'http': '111.77.196.174'},	{'http': '123.138.89.130'},	{'http': '69.4.87.58'},	{'http': '110.53.202.32'},	{'http': '101.255.84.182'},	{'http': '183.45.172.31'},	{'http': '186.117.128.92'},	{'http': '121.69.38.106'},	{'http': '218.6.145.11'},	{'http': '124.205.92.74'},	{'http': '103.63.159.186'},	{'http': '150.107.105.98'},	{'http': '149.202.198.179'},	{'http': '113.74.82.81'},	{'http': '181.143.103.170'},	{'http': '121.17.126.68'},	{'http': '124.200.102.149'},	{'http': '114.115.218.152'},	{'http': '110.73.182.12'},	{'http': '123.138.216.92'},	{'http': '31.146.184.18'},	{'http': '121.10.141.149'},	{'http': '185.112.148.142'},	{'http': '36.66.114.249'},	{'http': '203.198.193.3'},	{'http': '43.224.117.202'},	{'http': '103.4.147.82'},	{'http': '182.253.31.66'},	{'http': '101.86.112.186'},	{'http': '119.194.30.183'},	{'http': '154.72.192.154'},	{'http': '109.73.32.198'},	{'http': '203.151.206.27'},	{'http': '76.65.193.205'},	{'http': '159.255.167.131'},	{'http': '103.228.152.13'},	{'http': '187.44.1.167'},	{'http': '223.68.1.38'},	{'http': '128.140.33.65'},	{'http': '185.101.230.115'},	{'http': '123.144.120.21'},	{'http': '118.98.124.82'},	{'http': '111.155.116.200'},	{'http': '166.111.77.32'},	{'http': '134.236.16.234'},	{'http': '111.13.7.116'},	{'http': '202.162.203.175'},	{'http': '111.40.84.73'},	{'http': '218.25.125.146'},	{'http': '14.141.93.162'},	{'http': '85.202.11.47'},	{'http': '61.153.232.218'},	{'http': '27.46.74.43'},	{'http': '203.124.47.210'},	{'http': '165.16.32.1'},	{'http': '202.131.246.86'},	{'http': '5.200.72.154'},	{'http': '125.38.39.66'},	{'http': '69.4.87.56'},	{'http': '103.37.168.99'},	{'http': '218.108.107.70'},	{'http': '190.151.10.226'},	{'http': '182.253.106.14'},	{'http': '103.35.171.73'},	{'http': '41.211.104.203'},	{'http': '1.119.129.2'},	{'http': '125.31.19.26'},	{'http': '218.84.70.192'},	{'http': '117.176.62.148'},	{'http': '197.211.45.4'},	{'http': '60.190.96.190'},	{'http': '221.237.154.57'},	{'http': '176.109.2.15'},	{'http': '64.150.191.81'},	{'http': '120.132.71.212'},	{'http': '1.179.198.37'},	{'http': '220.249.185.178'},	{'http': '37.233.59.6'},	{'http': '115.183.11.158'},	{'http': '111.235.159.14'},	{'http': '109.185.180.87'},	{'http': '119.130.115.226'},	{'http': '103.240.8.2'},	{'http': '117.71.53.247'},	{'http': '60.191.134.165'},	{'http': '123.57.184.70'},	{'http': '1.82.216.135'},	{'http': '183.56.177.130'},	{'http': '144.217.31.225'},	{'http': '112.33.7.9'},	{'http': '101.86.86.101'},	{'http': '118.178.124.33'},	{'http': '103.12.246.12'},	{'http': '101.22.200.12'},	{'http': '61.191.41.130'},	{'http': '61.160.233.8'},	{'http': '210.101.131.231'},	{'http': '58.59.68.91'},	{'http': '58.54.227.31'},	{'http': '121.42.176.133'},	{'http': '113.108.253.195'},	{'http': '1.179.156.233'},	{'http': '101.37.79.125'},	{'http': '180.97.235.30'},	{'http': '218.75.116.58'},	{'http': '122.228.179.178'},	{'http': '114.115.218.71'},	{'http': '42.201.248.154'},	{'http': '27.46.74.40'},	{'http': '1.202.193.60'},	{'http': '117.184.149.66'},	{'http': '222.161.3.163'},	{'http': '118.190.14.107'},	{'http': '139.129.94.241'},	{'http': '218.56.132.155'},	{'http': '103.15.141.182'},	{'http': '190.185.165.225'},	{'http': '106.186.22.65'},	{'http': '222.249.233.238'},	{'http': '177.189.240.163'},	{'http': '36.97.145.29'},	{'http': '125.31.19.27'},	{'http': '103.30.90.206'},	{'http': '121.40.108.76'},	{'http': '148.101.86.162'},	{'http': '45.123.41.138'},	{'http': '201.16.243.189'},	{'http': '112.91.218.21'},	{'http': '183.62.71.242'},	{'http': '106.0.7.81'},	{'http': '221.217.17.205'},	{'http': '162.243.77.68'},	{'http': '117.24.5.73'},	{'http': '111.62.251.25'},	{'http': '111.79.199.115'},	{'http': '121.248.112.20'},	{'http': '121.41.175.199'},	{'http': '119.254.84.90'},	{'http': '219.150.242.54'},	{'http': '182.254.246.46'},	{'http': '111.13.2.131'},	{'http': '5.2.243.134'},	{'http': '1.179.183.86'},	{'http': '115.127.22.149'},	{'http': '180.173.109.149'},	{'http': '110.183.238.145'},	{'http': '220.248.229.45'},	{'http': '117.114.153.242'},	{'http': '61.152.81.193'},	{'http': '170.80.85.1'},	{'http': '111.13.7.122'},	{'http': '144.48.109.165'},	{'http': '121.8.243.51'},	{'http': '31.145.83.218'},	{'http': '193.84.184.25'},	{'http': '111.85.105.78'},	{'http': '60.214.154.2'},	{'http': '203.76.220.134'},	{'http': '27.46.74.59'},	{'http': '82.77.17.28'},	{'http': '103.12.220.29'},	{'http': '61.163.39.70'},	{'http': '111.13.109.27'},	{'http': '103.76.50.182'},	{'http': '116.214.32.51'},	{'http': '158.69.115.201'},	{'http': '124.202.247.110'},	{'http': '101.200.156.37'},	{'http': '1.82.216.134'},	{'http': '113.105.80.61'},	{'http': '121.52.209.76'},	{'http': '89.25.183.182'},	{'http': '45.115.168.40'},	{'http': '202.154.190.234'},	{'http': '111.13.7.118'},	{'http': '123.135.183.209'},	{'http': '113.200.159.155'},	{'http': '92.247.127.177'},	{'http': '218.56.132.154'},	{'http': '119.90.248.245'},	{'http': '115.124.73.122'},	{'http': '183.45.172.11'},	{'http': '81.23.0.41'},	{'http': '117.120.7.93'},	{'http': '187.60.173.6'},	{'http': '60.169.19.66'},	{'http': '121.13.165.22'},	{'http': '119.57.105.241'},	{'http': '60.191.170.148'},	{'http': '84.232.239.187'},	{'http': '117.143.109.132'},	{'http': '117.143.109.139'},	{'http': '120.198.138.106'},	{'http': '183.222.102.105'},	{'http': '186.155.197.172'},	{'http': '218.104.148.157'},	{'http': '111.1.3.38'},	{'http': '110.73.7.20'},	{'http': '5.102.176.2'},	{'http': '110.73.10.35'},	{'http': '41.33.22.186'},	{'http': '5.196.68.118'},	{'http': '60.13.143.99'},	{'http': '120.27.113.72'},	{'http': '159.203.37.37'},	{'http': '182.253.125.3'},	{'http': '193.227.49.83'},	{'http': '213.170.81.87'},	{'http': '46.164.141.45'},	{'http': '101.255.84.181'},	{'http': '103.56.233.147'},	{'http': '113.58.232.223'},	{'http': '116.211.143.11'},	{'http': '116.90.230.221'},	{'http': '118.97.188.117'},	{'http': '119.57.105.231'},	{'http': '120.199.64.163'},	{'http': '123.206.93.108'},	{'http': '125.93.148.196'},	{'http': '139.209.100.47'},	{'http': '163.125.244.28'},	{'http': '193.24.196.152'},	{'http': '200.25.233.166'},	{'http': '201.76.166.171'},	{'http': '113.121.181.198'},	{'http': '117.143.109.145'},	{'http': '117.143.109.154'},	{'http': '119.180.175.168'},	{'http': '122.152.198.191'},	{'http': '183.234.143.242'},	{'http': '31.145.83.198'},	{'http': '46.101.168.186'},	{'http': '138.201.63.123'},	{'http': '101.255.84.183'},	{'http': '27.46.74.54'},	{'http': '118.151.212.1'},	{'http': '218.56.132.156'},	{'http': '114.115.218.143'},	{'http': '111.13.7.123'},	{'http': '210.76.163.216'},	{'http': '183.78.182.228'},	{'http': '45.249.8.14'},	{'http': '105.27.177.18'},	{'http': '94.73.229.105'},	{'http': '193.19.243.246'},	{'http': '103.54.25.249'},	{'http': '67.148.156.107'},	{'http': '27.46.74.37'},	{'http': '125.38.39.40'},	{'http': '212.91.188.165'},	{'http': '110.232.72.179'},	{'http': '218.241.234.48'},	{'http': '154.65.4.90'},	{'http': '148.101.221.218'},	{'http': '202.69.38.82'},	{'http': '125.209.67.74'},	{'http': '123.133.67.82'},	{'http': '50.234.143.250'},	{'http': '195.138.86.112'},	{'http': '85.10.53.90'},	{'http': '106.248.234.43'},	{'http': '1.9.78.19'},	{'http': '111.1.3.34'},	{'http': '1.82.132.75'},	{'http': '171.8.79.91'},	{'http': '210.4.69.54'},	{'http': '218.60.55.3'},	{'http': '39.77.64.20'},	{'http': '49.73.78.80'},	{'http': '58.19.63.55'},	{'http': '61.53.65.54'},	{'http': '74.8.76.120'},	{'http': '78.90.86.13'},	{'http': '101.4.136.34'},	{'http': '103.58.74.78'},	{'http': '110.72.16.83'},	{'http': '110.73.6.241'},	{'http': '115.111.62.6'},	{'http': '118.26.9.179'},	{'http': '120.27.49.85'},	{'http': '120.83.13.53'},	{'http': '120.9.12.183'},	{'http': '120.9.16.193'},	{'http': '123.7.177.20'},	{'http': '124.42.7.103'},	{'http': '168.194.26.1'},	{'http': '185.36.60.68'},	{'http': '193.58.241.7'},	{'http': '210.203.20.9'},	{'http': '218.60.55.78'},	{'http': '221.14.7.241'},	{'http': '222.66.76.70'},	{'http': '223.15.34.76'},	{'http': '36.67.24.255'},	{'http': '36.73.111.81'},	{'http': '36.78.129.99'},	{'http': '39.77.29.128'},	{'http': '39.77.29.146'},	{'http': '41.46.28.109'},	{'http': '5.197.202.69'},	{'http': '58.221.38.70'},	{'http': '58.252.6.165'},	{'http': '59.44.29.249'},	{'http': '61.135.217.3'},	{'http': '61.176.215.7'},	{'http': '64.137.222.8'},	{'http': '83.166.96.47'},	{'http': '85.93.143.51'},	{'http': '103.244.7.222'},	{'http': '110.73.193.89'},	{'http': '110.88.223.39'},	{'http': '112.82.232.91'},	{'http': '113.128.91.32'},	{'http': '113.66.158.95'},	{'http': '115.127.75.18'},	{'http': '115.212.61.20'},	{'http': '116.16.105.21'},	{'http': '116.23.137.66'},	{'http': '117.24.20.178'},	{'http': '120.194.18.90'},	{'http': '120.52.21.132'},	{'http': '121.15.220.61'},	{'http': '121.78.182.47'}]
proxy=choice(proxy_list)
print(proxy)