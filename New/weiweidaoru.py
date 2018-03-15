# -*- coding:utf-8 -*-
import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
from engine import *
df = pd.read_excel('C:\\Users\\63220\\Desktop\\分类\\测试.csv')
df['stype_code']=df['stype_code'].apply(lambda x: '%.0f' % x)
df['confirmed']=df['confirmed'].apply(lambda x: '%.0f' % x)
print(df)
lol = df.values.tolist()
print(lol)
dict1={'0':'0000',
       '1':'0001',
       '10':'0010',
       '11':'0011'}
dict2={60101:'股票策略',
        60101:'股票策略',
        60101:'股票策略',
        60102:'管理期货',
        60102:'管理期货',
        60102:'管理期货',
        60103:'相对价值',
        60103:'相对价值',
        60103:'相对价值',
        60103:'相对价值',
        60103:'相对价值',
        60103:'相对价值',
        60104:'事件驱动',
        60104:'事件驱动',
        60104:'事件驱动',
        60104:'事件驱动',
        60105:'债券策略',
        60106:'宏观策略',
        60107:'组合策略',
        60107:'组合策略',
        60107:'组合策略',
        60107:'组合策略',
        60108:'多策略',
        60109:'其他一级策略',
        60109:'其他一级策略',
        60109:'其他一级策略',
        60109:'其他一级策略'}
dict3={60501:'证券投资基金',
        60502:'股权投资基金',
        60503:'创业投资基金',
        60504:'其他投资基金'}

dict4={'6010101':'股票多头',
        '6010102':'股票多空',
        '6010103':'市场中性',
        '6010201':'期货趋势',
        '6010202':'期货套利',
        '6010203':'其他管理期货策略',
        '6010301':'ETF套利',
        '6010302':'可转债套利',
        '6010303':'固定收益套利',
        '6010304':'分级基金套利',
        '6010305':'其他相对价值策略',
        '6010306':'期权套利',
        '6010401':'并购重组',
        '6010402':'定向增发',
        '6010403':'大宗交易',
        '6010404':'其他事件驱动策略',
        '6010501':'债券策略',
        '6010601':'宏观策略',
        '6010701':'MOM',
        '6010702':'FOF',
        '6010703':'TOT',
        '6010704':'其他组合策略',
        '6010801':'多策略',
        '6010901':'其他二级策略',
        '6010902':'新三板',
        '6010903':'海外基金',
        '6010904':'货币基金'}



# ww =[['JR153181', 60502, 'nan', '11', '蒋伟伟'], ['JR153183', 60502, 'nan', '11', '蒋伟伟'],['JR138779', 60101, '6010101', '11', '蒋伟伟'], ['JR136166', 60101, '6010101', '11', '蒋伟伟']]
# x=['JR153181', 60502, 'nan', '11', '蒋伟伟']



#
# if x[3] in dict1:
#     print(dict1[3])
dataframe1=[]
dataframe2=[]
for w in lol:
    ty=w[1]
    print(ty)
    if ty in dict3:
        dd1=[]
        dd1.append(w[0])
        dd1.append(w[1])
        type_name=dict3[ty]
        dd1.append(type_name)
        ll = w[3]
        diyi = dict1[ll]
        dd1.append(diyi)
        dd1.append(w[4])
        aa='605'
        bb='按基金类型分类'
        dd1.append(aa)
        dd1.append(bb)
        dataframe1.append(dd1)

    else:
        dd2 = []
        dd2.append(w[0])
        dd2.append(w[1])
        type_name = dict2[ty]
        dd2.append(type_name)
        ll = w[3]
        if ll in dict1:
            diyi = dict1[ll]
            dd2.append(diyi)
        else:
            dd2.append(ll)
        dd2.append(w[4])
        yy=w[2]
        stype_name=dict4[yy]
        dd2.append(yy)
        dd2.append(stype_name)
        aa = '601'
        bb = '按投资策略分类'
        dd2.append(aa)
        dd2.append(bb)
        dataframe2.append(dd2)
print(dataframe1)
print(dataframe2)

df2 = pd.DataFrame(dataframe2)

if dataframe1==[]:
    print("没有605")
else:
    df1 = pd.DataFrame(dataframe1)
    df1.columns = ["fund_id", "type_code", "type_name", "confirmed", "classified_by", "typestandard_code","typestandard_name"]

df2.columns=["fund_id","type_code","type_name", "confirmed","classified_by","stype_code","stype_name","typestandard_code","typestandard_name"]

print(df2)



is_checked = input("输入1来确认入库,输入2确认605策略\n")
if is_checked == "1":

    to_sql("fund_type_mapping_import", engine_base, df2, type="update")
elif is_checked == "2":

    to_sql("fund_type_mapping_import", engine_base, df1, type="update")
else:
    pass



















