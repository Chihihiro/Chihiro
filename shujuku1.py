import pymysql
import pandas as pd

connect = pymysql.connect(  # 连接数据库服务器
    user="jr_admin_4",
    password="jr_admin_4",
    host="182.254.128.241",
    port=4171,
    db="base",
    charset="utf8"
)
conn = connect.cursor()  # 创建操作游标
# 你需要一个游标 来实现对数据库的操作相当于一条线索

                         # 查看
conn.execute("SELECT fund_id,fund_name,fund_full_name FROM fund_info WHERE fund_id like 'JR00000%'")  # 选择查看自带的user这个表  (若要查看自己的数据库中的表先use XX再查看)
rows = conn.fetchall()  # fetchall(): 接收全部的返回结果行，若没有则返回的是表的内容个数 int型
info=[]
for i in rows:
    # v =str(i)
    info.append(i[0])

# print(i[1])
print(info)
# -------------------------------------------------------------------------------------------------------------不需要
# con.execute('SET NAMES UTF8')
# con.execute("drop database douban")  # 以下7行表示删除原有的数据库和其中的表，新建数据库和表
# con.execute("create database douban")
# con.execute("use douban")  # 使用douban这个数据库
# con.execute("drop table if exists t_doubantop")
# sql = '''''create table t_doubantop(num BIGINT,name VARCHAR(40) NOT NULL,charactor VARCHAR(40),remark VARCHAR(40),score VARCHAR(20))'''
# con.execute(sql)  # sql中的字符表示创建一个表 对应的信息有   num  name  charactor  remark  score
#
# conn.execute("drop database if exists new_database")   #如果new_database数据库存在则删除
# conn.execute("create database new_database")   #新创建一个数据库
# conn.execute("use douban") #选择表
# sql 中的内容为创建一个名为new_table的表
# sql = """create table new_table(id BIGINT,name VARCHAR(20),age INT DEFAULT 1)"""  #()中的参数可以自行设置
# sql = """create table t_doubantop(num BIGINT,name VARCHAR(40) NOT NULL,charactor VARCHAR(40),remark VARCHAR(40),score VARCHAR(20))"""
# conn.execute("drop table if exists new_table") # 如果表存在则删除
# conn.execute(sql)   # 创建表
# --------------------------------------------------------------------------------------------------------------------------不需要

# exit()
allinfo=[]
# info=['JR00001','JR000002','JR000003','JR000004','JR000005','JR000006','JR000012','JR002002','JR000022']

result = pd.DataFrame(columns=['fund_id',
                               'typestandard_code',
                               'typestandard_name',
                               'type_code',
                               'type_name',
                               'stype_code',
                               'stype_name'
                               ])

k = [600, '按全产品分类', 60001, '全产品', 6000101, '全产品']
temp = [k]*len(info)
result.loc[:, 'fund_id'] = info
result.loc[:, [ 'typestandard_code',
                               'typestandard_name',
                               'type_code',
                               'type_name',
                               'stype_code',
                               'stype_name']] = temp
# for i in range(len(k)):
#     result.iloc[:, i+1] = k[i]

print(result)
# result.to_csv("C:\\Users\\63220\\Desktop\\test我全产品测试.csv")
# exit()

# for x in info:
#     print(x)
#     k = [600, '按全产品分类', 60001, '全产品', 6000101, '全产品']
#     k.insert(0,x)
#     print(k)
#     allinfo.append(k)
# print(allinfo)


#
# connect = pymysql.connect(  # 连接数据库服务器
#     user="root",
#     password="",
#     host="localhost",
#     port=3306,
#     db="test",
#     charset="utf8"
# )
# conn = connect.cursor()  # 创建操作游标
# conn.source(result)
# conn.close()           #   关闭游标连接
# connect.close()        #   关闭数据库服务器连接 释放内存

