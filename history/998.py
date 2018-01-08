import requests
import re
#
#
# session = requests.Session()
#
# prarams ={'username':'15026588463','password':'chihiro123'}
#
# s = session.post("https://api.growingio.com/v2/9907c51ef09823c8d5b98c511e30a866/web/action?stm=1511340260948",prarams)
#
# print(s.cookies.get_dict())
#
import requests
import re
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
url = 'https://www.jinfuzi.com/product/05P61l7o8edk.html'
r = requests.get(url, headers=headers).content.decode('utf8')
print(r)


exit()

R = re.search('<div class="clearfix">.*?<h1 class="title">(.+?)</h1>', r, re.DOTALL).group(1)

print(R)