from sqlalchemy import Column, String, create_engine
import os
import pandas as pd
import re
import pymysql

res = "某某先生, 从事基金10年"
res = "某某, 性别男, 从事基金10年"
patt_gender = "先生|男"
re.search(patt_gender, res).group()
{
    "先生":"男"
}

df=pd.read_excel('C:\\Users\\63220\Desktop\\na11me.xlsx')
# df=df1["姓名","性别"]

cols_used = ["名字","性别"]
result = df[cols_used]
print(type(result))
# t=result.set_index('名字')
# t=result.set_columns('性别')
dict = {
    key: value for key, value in zip(result["名字"], result["性别"])
}

# dict=result.to_dict(orient='records')
# dict=df.to_dict()
print(dict)
# exit()
x='玄'
y=dict[x]

print(y)

