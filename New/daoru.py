from New.iosjk import to_sql
from engine import *

# -------------------------------------------------------------------------朝阳
# def to_zyjz(fund_id):
#     df = pd.read_excel('C:\\Users\\63220\\Desktop\\产品详情-历史净值-2.xls')
#     df.columns = ["statistic_date", "nav", "added_nav", "swanav1","source"]
#     # print(df)
#     df['source'] = "第三方"
#     df['source_code']=3
#     df['data_source']=2
#     df['data_source_name']='朝阳永续'
#     df.drop(['swanav1'], axis=1,inplace=True)
#     df['fund_id']=fund_id
#     dataframe = df
#     print(dataframe)
#     is_checked = input("输入1来确认入库\n")
#     if is_checked == "1":
#         to_sql("fund_nv_data_source", engine5, dataframe, type="update")
#     else:
#         pass
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
df = pd.read_excel('C:\\Users\\63220\Desktop\\org_info补充.xlsx')
# df["jfz_timeid"]=df["jfz_timeid"].apply(lambda x:'%.0f' % x if x is not None else None)
# df["source"]=df["source"].apply(lambda x: '0'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: int(x))

# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: '0%.0f' % x)
# df["source"]=df["source"].apply(lambda x: '0'+str(x))
# df['num_employee'] = df['num_employee'].apply(lambda x: '%.2f' % x)
# df["confirmed"]=df["confirmed"].apply(lambda x: '00'+str(x))
# df["fund_id"]=df["fund_id"].apply(lambda x: "%06d" % x)
# df.rename(columns={"is_used":"is_updata"},inplace=True)
# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
df["version"]=2018031411
# df["target_table"]="fund_nv_data_standard"
# print(df)
# exit()
# df=pd.read_sql("select fund_id,typestandard_name,type_code,type_name,stype_code from fund_type_mapping_import where stype_code=6020204 and stype_name not like '中间级（夹层）'",engine_base)
# df["typestandard_code"]=602
# df["stype_name"]="中间级（夹层）"
# df["target_table"]="fund_nv_data_standard"

# df["source_id"]='020007'
print(df)
def sync_source(df):
    df2=df.iloc[:,[0,2]]
    df2.rename(columns={"matched_id":"pk","source":"source_id"},inplace=True)
    df2["priority"]=9
    df2["target_table"]="fund_nv_data_standard"
    return df2
# df2=sync_source(df)
# print(df2)
# ------------------------------------------------------------------txt

dataframe = df
is_checked = input("输入1,2来确认入库\n")
if is_checked == "1":
    engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
    to_sql("y_org_info", engine_crawl_private, dataframe, type="update")   #ignore
else:
    pass
#
# if is_checked == "2":
#     to_sql("sync_source", engine_config_private, df2, type="update")  # ignore
# else:
#     pass
