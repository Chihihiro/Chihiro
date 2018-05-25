from New.iosjk import to_sql
from engine import *

df = pd.read_excel('C:\\Users\\63220\Desktop\\id_match20180520.xls', dtype=str)  # dtype=str
# df["jfz_timeid"]=df["jfz_timeid"].apply(lambda x:'%.0f' % x if x is not None else None)
# df["confirmed"]=df["confirmed"].apply(lambda x: '00'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: int(x))
# df["source_id"]=df["source_id"].apply(lambda x: '0'+str(x))
# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
# df["source_id"]=df["source_id"].apply(lambda x: '0%.0f' % x)
# df["source"]=df["source"].apply(lambda x: '0'+str(x))
# df['num_employee'] = df['num_employee'].apply(lambda x: '%.2f' % x)
# df["confirmed"]=df["confirmed"].apply(lambda x: '00'+str(x))
# df["org_code"]=df["org_code"].apply(lambda x: x.zfill(9))
# df.rename(columns={"is_used":"is_updata"},inplace=True)
# df["matched_id"]=df["matched_id"].apply(lambda x: '0'+str(x))
# df["version"]=2018032211
# df["target_table"]="fund_nv_data_standard"
# df["data_source"]='000001'
# df["typestandard_code"]=df["typestandard_code"].apply(lambda x: '0'+str(x))
# df["target_table"]="fund_nv_data_standard"
# df["version"]=2018040316
# df["source_id"]='020007'
print(df)


def sync_source(df):
    df2 = df.iloc[:, [0, 2]]
    df2.rename(columns={"matched_id": "pk", "source": "source_id"}, inplace=True)
    df2["priority"] = 9
    df2["target_table"] = "fund_nv_data_standard"
    return df2


# df2=sync_source(df)
# print(df2)
# ------------------------------------------------------------------txt

dataframe = df
is_checked = input("输入1,2来确认入库\n")
if is_checked == "1":
    engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                            connect_args={"charset": "utf8"}, echo=True, )
    to_sql("id_match", engine_base, dataframe, type="update")  # ignore
else:
    pass


#
# if is_checked == "2":
#     to_sql("sync_source", engine_config_private, df2, type="update")  # ignore
# else:
#     pass

# df = pd.read_excel('C:\\Users\\63220\Desktop\\删org）mapping.xls')
def Del_org_mapping(JR, org_id, type):
    return engine_base.execute(
        "DELETE from fund_org_mapping WHERE fund_id='{}' AND org_id='{}' AND org_type_code='{}'".format(JR, org_id,
                                                                                                        type))


import tushare

