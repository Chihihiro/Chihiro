from New.iosjk import to_sql
from engine import *

df = pd.read_excel('C:\\Users\\63220\Desktop\\【0401】（导入数据库）公募分类已确定.xlsx',dtype=str)#dtype=str
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

df = pd.read_sql("SELECT * FROM fund_type_mapping_source where entry_time like '2018-04-02%%'", engine_base_public)
dict_type = {'01': '运行方式',
             '02': '投资标的',
             '03': '投资期限',
             '04': '投资方式',
             '05': '分级基金'}

df['typestandard_name'] = df['typestandard_code'].apply(lambda x: dict_type.get(x))
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
    to_sql("fund_type_mapping_source", engine_base_public, dataframe, type="update")  # ignore
else:
    pass
