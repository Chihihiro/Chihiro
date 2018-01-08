def get_html(web_url):       #爬虫获取网页没啥好说的
    header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.108 Safari/537.36 2345Explorer/8.5.1.15355"}
    html = requests.get(url=web_url,headers=header).text
    Soup = BeautifulSoup(html,"lxml")
    data = Soup.find("ol").find_all("li")   #还是有一点要说，就是返回的信息最好只有你需要的那部分，所以这里进行了筛选
    return data

def get_info(all_move):
    for info in all_move:
        #    编号
        nums = re.findall(r'<em class="">\d+</em>',str(info),re.S|re.M) #编号我使用的是正则表达式来获取
        nums = re.findall(r'\d+',str(nums),re.S|re.M)
        num = nums[0]

        #    名字
        names = info.find("span")   #名字比较简单 偷了一下懒直接获取第一个span就是
        name = names.get_text()

        #    导演
        charactors = info.find("p")      #这段信息中有太多非法符号你需要替换掉
        charactor = charactors.get_text().replace(" ","").replace("\n","")   #使信息排列规律
        charactor = charactor.replace("\xa0","").replace("\xee","").replace("\xf6","").replace("\u0161","").replace("\xf4","").replace("\xfb","").replace("\u2027","")
        charactor = charactor[0:30]  #由于本人不才，数据库才入门所以需要控制信息长度，以免存入超范围  (大神忽略)

        #    评语
        remarks = info.find_all("span",{"class":"inq"})
        print(remarks)
        if remarks:          #这个判断是因为有的电影没有评语，你需要做判断
            remark = remarks[0].get_text().replace("\u22ef","")
            remark = remark[0:30]  #同上面一个
        else:
            remark = "此影片没有评价"

        #    评分
        scores = info.find_all("span",{"class":"rating_num"})   #没啥好说 匹配就行
        score = scores[0].get_text()


        f = open("F:\\Pythontest1\\douban.txt","a")   #将上面的信息每一行以按逗号分隔的规律存入本地
        f.write(num+",")
        f.write(name+",")
        f.write(charactor+",")
        f.write(remark+",")
        f.write(score)
        f.write("\n")
    f.close()         #记得关闭文件