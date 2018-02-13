import pandas as pd
from History.iosjk import to_sql
from sqlalchemy import create_engine

engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )

df=pd.read_sql("SELECT	*  FROM fund_type_source WHERE typestandard_code = '801' AND fund_id NOT IN ( SELECT	fund_id FROM fund_type_mapping WHERE typestandard_code = '605') AND type_code IN ('80101', '80104', '80103','80107')",engine2)

df['type_code']=df['type_name']

dict={'证券投资基金':'60501',
        '股权投资基金':'60502',
        '创业投资基金':'60503',
        '其他投资基金':'60504'}
dict1= {801:'605'}
dict2={'按发行主体分类':'按基金类型分类'}

df["type_code"]=df["type_code"].apply(lambda x: dict.get(x))
df["typestandard_code"]=df["typestandard_code"].apply(lambda x: dict1.get(x))
df["typestandard_name"]=df["typestandard_name"].apply(lambda x: dict2.get(x))




code_use=["fund_id","fund_name","typestandard_code","typestandard_name","type_code","type_name"]
result=df[code_use]

print(result)




dataframe=result
is_checked = input("输入1来确认入库\n")
if is_checked == "1":
    # engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd','jr_admin_qxd','182.254.128.241',4171,'config_private', ), connect_args={"charset": "utf8"},echo=True,)
    engine = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )
    # engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
    to_sql("fund_type_mapping_import", engine, dataframe, type="update")
else:
    pass






