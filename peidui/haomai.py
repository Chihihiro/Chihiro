from engine import *


def df_haomai():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_full_name,reg_code \
     FROM d_fund_info WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020001') \
    and source_id = '020001'  \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "source_id", "fund_full_name": "test_name"
        , "reg_code": "jfz_reg_code", "foundation_date": "jfz_foundation_date"}, inplace=True)
    return df_fundaccount


def df_info():
    info_fund_name = pd.read_sql("select fund_id,fund_full_name,reg_code,foundation_date from fund_info ", engine_base)
    return info_fund_name


table_reg_code = pd.read_sql("select reg_code,fund_id from fund_info where reg_code is not NULL", engine_base)
table_reg_code["reg_code"] = table_reg_code["reg_code"].apply(lambda x: x.strip())
dict = {key: value for key, value in zip(table_reg_code["reg_code"], table_reg_code["fund_id"])}


def test2(df):
    # df["fund_id"] = df["test_name"].apply(lambda x: dict1.get(x))
    df["fund_id"] = df["jfz_reg_code"].apply(lambda x: dict.get(x))
    # df["foundation_test"] = df["fund_id2"].apply(lambda x: dict.get(x))
    return df


df = df_haomai()
a = test2(df)
df1 = a.loc[a["fund_id"].notnull()]
new = df1.loc[:, ["fund_id", "source_id"]]
new["id_type"] = 1
new["source"] = '020001'
new["is_used"] = 1
new["is_del"] = 0
new.rename(columns={"fund_id": "matched_id"}, inplace=True)
# to_sql("id_match", engine_base, new, type="update")  # ignore
