from engine import *


def df_jfz():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT 	fund_id,fund_full_name,foundation_date,reg_code \
    FROM d_fund_info WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020002') \
    and source_id = '020002' and version>10 AND is_used=1  \
    ORDER BY version DESC ) AS T GROUP  BY T.fund_id", engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "source_id", "fund_full_name": "test_name"
                                   ,"reg_code":"jfz_reg_code","foundation_date":"jfz_foundation_date"}, inplace=True)
    return df_fundaccount



def df_info():
    info_fund_name = pd.read_sql("select fund_id,fund_full_name,reg_code,foundation_date from fund_info ", engine_base)
    return info_fund_name


dict_table = {"010002": "x_fund_info_fundaccount",
              "010003": "x_fund_info_private",
              "010004": "x_fund_info_securities",
              "010005": "x_fund_info_futures",
              "020001": "d_fund_info",
              "020002": "d_fund_info",
              "020003": "d_fund_info",
              }


def fund_full_name(fund_id):
    df = pd.read_sql("select source_id,source from id_match WHERE matched_id='{}' \
    and is_used=1 AND source not in ('010001','000001','020004','020005','020007') and source not like '03%%' and source not like'04%%' and source not like'05%%'".format(
        fund_id), engine_base)
    num = len(df)

    df['fund_tabel'] = df['source'].apply(lambda x: dict_table.get(x))
    df["fund_full_name"] = None
    for i in range(num):
        a = df.iloc[i, 0]
        b = df.iloc[i, 1]
        c = df.iloc[i, 2]
        if b in ('020001', '020002', '020003'):
            name = pd.read_sql(
                "SELECT DISTINCT fund_full_name FROM d_fund_info WHERE fund_id='{}' and source_id='{}'".format(a, b),
                engine_crawl_private)
            try:
                full_name = name.iloc[0, 0]
            except BaseException:
                pass
            else:
                full_name = '空'
            df.iloc[i, 3] = full_name

        else:
            name = pd.read_sql("SELECT DISTINCT fund_name_amac FROM {} where fund_id='{}'".format(c, a),
                               engine_crawl_private)
            full_name = name.iloc[0, 0]
            df.iloc[i, 3] = full_name
    print(df)
    L = df["fund_full_name"]
    list = to_list(L)
    return list

table_reg_code = pd.read_sql("select reg_code,fund_id from fund_info where reg_code is not NULL", engine_base)
table_reg_code["reg_code"] = table_reg_code["reg_code"].apply(lambda x: x.strip())
dict = {key: value for key, value in zip(table_reg_code["reg_code"], table_reg_code["fund_id"])}


def test2(df):
    # df["fund_id"] = df["test_name"].apply(lambda x: dict1.get(x))
    df["fund_id"] = df["jfz_reg_code"].apply(lambda x: dict.get(x))
    # df["foundation_test"] = df["fund_id2"].apply(lambda x: dict.get(x))
    return df



df=df_jfz()
a=test2(df)
df1 =a.loc[a["fund_id"].notnull()]
new=df1.loc[:,["fund_id","source_id"]]
new["id_type"]=1
new["source"]='020002'
new["is_used"]=1
new["is_del"]=0
new.rename(columns={"fund_id":"matched_id"},inplace=True)
to_sql("id_match", engine_base, new, type="update")  # ignore


# c=pd.read_sql("SELECT t.source_id,t.fund_id,t.fund_name,c.name_detail,t.count,t.min from  \
# (select  DISTINCT source_id,fund_id,fund_name,COUNT(fund_id) as count, \
# MIN(statistic_date) as min from s_fund_nv where  fund_id not in (SELECT source_id from base.id_match where id_type=1) and is_used=1 \
# GROUP  BY fund_id) AS t  \
# LEFT JOIN config_private.source_info AS c on t.source_id=c.source_id;",engine_crawl_private)