import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
from engine import *

# -------------------------------------------------------------------------朝阳
def to_zyjz(fund_id):
    df = pd.read_excel('C:\\Users\\63220\\Desktop\\产品详情-历史净值-2.xls')
    df.columns = ["statistic_date", "nav", "added_nav", "swanav1","source"]
    # print(df)
    df['source'] = "第三方"
    df['source_code']=3
    df['data_source']=2
    df['data_source_name']='朝阳永续'
    df.drop(['swanav1'], axis=1,inplace=True)
    df['fund_id']=fund_id
    dataframe = df
    print(dataframe)
    is_checked = input("输入1来确认入库\n")
    if is_checked == "1":
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_4','jr_admin_4','182.254.128.241',4171,'base', ), connect_args={"charset": "utf8"},echo=True,)
        to_sql("fund_nv_data_source", engine, dataframe, type="update")
    else:
        pass
# id="JR019585"
# print(to_zyjz(id))

# ------------------------------------------------------------------------------朝阳
# df=pd.read_excel('C:\\Users\\63220\\Desktop\\瓦洛兰投资净值(1).xlsx')
# # df["source_id"] = df["source_id"].apply(lambda x: "0" + str(x))
# df['nav'] = df['nav'].apply(lambda x: '%.4f' % x)
# df['added_nav'] = df['added_nav'].apply(lambda x: '%.4f' % x)
#
#
# print(df)

# ------------------------------------------------------------txt
# fileobj = open('C:\\Users\\63220\\Desktop\\id001.txt','r')
# print(fileobj)
# # print(id)
# exit()
# df2=pd.DataFrame(fileobj)
# df2=df2.T
# df2.columns=['source_id']
# # frames=[df,df2]
# print(df2)
#
# df=pd.read('C:\\Users\\63220\\Desktop\\id001.txt')
#
#
df = pd.read_excel('C:\\Users\\63220\Desktop\\私募id_match导入0209.xls')
# df["jfz_timeid"]=df["jfz_timeid"].apply(lambda x:'%.0f' % x if x is not None else None)
df["source"]=df["source"].apply(lambda x: '0'+str(x))
# df["data_source"]=df["data_source"].apply(lambda x: '0'+str(x))
# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: '00'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: '0'+str(x))
# df['num_employee'] = df['num_employee'].apply(lambda x: '%.2f' % x)
# df["confirmed"]=df["confirmed"].apply(lambda x: '00'+str(x))
# df["fund_id"]=df["fund_id"].apply(lambda x: "%06d" % x)
# df.rename(columns={"is_used":"is_updata"},inplace=True)
# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
# df["version"]=2018020511
# df["target_table"]="fund_nv_data_standard"
# print(df)
# exit()
# df=pd.read_sql("select fund_id,typestandard_name,type_code,type_name,stype_code from fund_type_mapping_import where stype_code=6020204 and stype_name not like '中间级（夹层）'",engine_base)
# df["typestandard_code"]=602
# df["stype_name"]="中间级（夹层）"
print(df)

# ------------------------------------------------------------------txt

dataframe=df
is_checked = input("输入1来确认入库\n")
if is_checked == "1":
    engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd','jr_admin_qxd','182.254.128.241',4171,'crawl_private', ), connect_args={"charset": "utf8"},echo=True,)
    engine_base = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )
    engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'config_private', ),connect_args={"charset": "utf8"}, echo=True, )
    engine_lijia = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base_public', ), connect_args={"charset": "utf8"}, echo=True, )
    engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
    engine_config_private = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'config_private', ),connect_args={"charset": "utf8"}, echo=True, )
    to_sql("id_match", engine_base, dataframe, type="update")
else:
    pass

