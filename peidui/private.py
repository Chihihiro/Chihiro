from engine import *


def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---|无|%|暂未备案", "", x)
    if s == "":
        return None
    else:
        return s


def df_fundaccount1():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name_amac,reg_code_amac \
     FROM x_fund_info_fundaccount WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010002' and is_used=1)\
    and entry_time>'2017-11-19' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id;", engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_fundaccount


def df_private1():
    df_private = pd.read_sql("SELECT * FROM (SELECT  fund_id \
    , fund_name_amac,reg_code_amac \
    FROM x_fund_info_private WHERE fund_id not in  \
    (SELECT source_id FROM base.id_match WHERE source='010003' and is_used=1) \
    and entry_time>'2017-11-20' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_private.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_private


def df_securities1():
    df_securities = pd.read_sql("SELECT * FROM (SELECT   fund_id \
    , fund_name_amac,reg_code_amac FROM x_fund_info_securities WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010004' and is_used=1)  \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T  \
    GROUP  BY T.fund_id;", engine_crawl_private)
    df_securities.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_securities


def df_futures():
    df_futures = pd.read_sql("SELECT * FROM (SELECT  fund_id, fund_name_amac,reg_code_amac \
     FROM x_fund_info_futures WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010005' and is_used=1) \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T  \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_futures.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_futures


def df_010006():
    df_futures = pd.read_sql("SELECT * FROM (SELECT  fund_id, fund_name,reg_code \
     FROM x_fund_info_010006 WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010006' and is_used=1) \
    ORDER BY version DESC ) AS T  \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_futures.rename(columns={"fund_id": "source_id", "fund_name": "test_name", "reg_code": "reg_code_amac"},
                      inplace=True)
    return df_futures


def df_020001():
    df = pd.read_sql("SELECT * FROM (SELECT fund_id, \
                fund_full_name, \
                reg_code \
                FROM d_fund_info  \
                WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020001') \
                and source_id = '020001'  \
                ORDER BY version DESC ) AS T \
                GROUP  BY T.fund_id", engine_crawl_private)
    df["reg_code"] = df["reg_code"].apply(lambda x: sub_wrong_to_none(x) if type(x) is str else x)
    df.rename(columns={"fund_id": "source_id", "fund_full_name": "test_name", "reg_code": "reg_code_amac"},
                      inplace=True)
    return df


def df_020002():
    df = pd.read_sql("select ma.fund_id,df.fund_full_name,df.reg_code FROM ( \
                        SELECT MAX(version) as mm, fund_id from crawl_private.d_fund_info WHERE source_id = '020002' and is_used = 1 GROUP BY fund_id) as ma \
                        JOIN crawl_private.d_fund_info as df \
                        ON ma.mm = df.version  \
                        and ma.fund_id = df.fund_id \
                        WHERE ma.fund_id NOT IN (SELECT source_id from base.id_match where id_type = 1 and source = '020002') \
                        AND ma.mm > 10", engine_crawl_private)
    df["reg_code"] = df["reg_code"].apply(lambda x: sub_wrong_to_none(x) if type(x) is str else x)
    df.rename(columns={"fund_id": "source_id", "fund_full_name": "test_name", "reg_code": "reg_code_amac"},
                      inplace=True)
    return df


dict_table = {"010002": "x_fund_info_fundaccount",
              "010003": "x_fund_info_private",
              "010004": "x_fund_info_securities",
              "010005": "x_fund_info_futures",
              "020001": "d_fund_info",
              "020002": "d_fund_info",
              "020003": "d_fund_info",
              }


def generate_id(start_from, length):
    ids = [(start_from + i) for i in range(0, length)]
    return ["JR" + (6 - len(str(x))) * "0" + str(x) for x in ids]


table_reg_code = pd.read_sql("select reg_code,fund_id from fund_info where reg_code is not NULL", engine_base)
table_reg_code["reg_code"] = table_reg_code["reg_code"].apply(lambda x: x.strip())
dict = {key: value for key, value in zip(table_reg_code["reg_code"], table_reg_code["fund_id"])}
yes = 'yes'
no = 'no'

table1 = pd.read_sql("select fund_full_name,fund_id from fund_info", engine_base)
dict1 = {key: value for key, value in zip(table1["fund_full_name"], table1["fund_id"])}


def fund_full_name(fund_id):
    df = pd.read_sql("select source_id,source from id_match WHERE matched_id='{}' \
    and is_used=1 AND source not in ('010001','000001','020004','020005','020007','020008') and source not like '03%%' and source not like'04%%' and source not like'05%%'".format(
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
                "SELECT DISTINCT fund_full_name FROM d_fund_info WHERE fund_id='{}' and source_id='{}' and  version>10".format(
                    a, b),
                engine_crawl_private)
            try:
                full_name = name.iloc[0, 0]
                df.iloc[i, 3] = full_name
            except BaseException:
                full_name = '空'
                df.iloc[i, 3] = full_name
            else:
                pass
        else:
            name = pd.read_sql("SELECT DISTINCT fund_name_amac FROM {} where fund_id='{}'".format(c, a),
                               engine_crawl_private)
            full_name = name.iloc[0, 0]
            df.iloc[i, 3] = full_name
    print(df)
    L = df["fund_full_name"]
    list = to_list(L)
    return list


def atest2(private_null):
    if private_null.empty:
        print("米有匹配到")
        c = []
        d = []
        return c, d
    else:
        print("have")
        # private_null["fund_id"] = private_null["test_name"].apply(lambda x: dict1.get(x))
        no = private_null.fillna("空")
        del no["fund_id"]
        no.rename(columns={"fund_id_get": "fund_id"}, inplace=True)
        c = no[no['fund_id'] != '空']
        d = no[no['fund_id'] == '空']

        return c, d


def atest1(private):
    if private.empty:
        print("米有匹配到")
        a = []
        b = []
        return a, b

    else:
        print("have")
        private["same"] = list(
            map(lambda x, y: yes if y in fund_full_name(x) else no, private["fund_id"], private["test_name"]))
        a = private[private["same"] == 'yes']
        b = private[private["same"] == 'no']
        return a, b


def re_len(a, b):
    if len(a) == 0 and len(b) > 0:
        result = b
        return result
    elif len(b) == 0 and len(a) > 0:
        result = a
        return result
    elif len(a) == 0 and len(b) == 0:
        result = []
        return result
    else:
        result = a.append(b)
        return result


def id_match(fund_info, source):
    # fund_info=df
    fund_info["fund_id"] = fund_info["reg_code_amac"].apply(lambda x: dict.get(x))
    private = fund_info.loc[fund_info["fund_id"].notnull()]
    private_null = fund_info.loc[fund_info["fund_id"].isnull()]
    t1 = atest1(private)
    t2 = atest2(private_null)
    a = t1[0]
    # b = t1[1] #pass 掉
    c = t2[0]
    d = t2[1]

    # result = pd.merge(a, c, how='outer')
    # result2 = pd.merge(b, d, how='outer')

    result = re_len(a, c)
    result2 = d

    if len(result) == 0:
        new_fund_id = result2["source_id"]
        new_fund = pd.DataFrame(new_fund_id)
        new_fund["source"] = source
        new_fund["id_type"] = 1
        new_fund["is_used"] = 1
        new_fund["is_del"] = 0
        jr = pd.read_sql("select max(matched_id) from id_match where  id_type=1", engine_base)
        maxjr = jr.iloc[0, 0]
        m = int(maxjr.replace("JR", ""))
        new_fund["matched_id"] = generate_id(m + 1, len(new_fund))
        return new_fund

    elif len(result2) == 0:
        new1 = result.loc[:, ['fund_id', 'source_id']]
        new1["source"] = source
        new1["id_type"] = 1
        new1["is_used"] = 1
        new1["is_del"] = 0
        new1.rename(columns={"fund_id": "matched_id"}, inplace=True)
        return new1
    else:
        new1 = result.loc[:, ['fund_id', 'source_id']]
        new1["source"] = source
        new1["id_type"] = 1
        new1["is_used"] = 1
        new1["is_del"] = 0
        new_fund_id = result2["source_id"]
        new_fund = pd.DataFrame(new_fund_id)
        new_fund["source"] = source
        new_fund["id_type"] = 1
        new_fund["is_used"] = 1
        new_fund["is_del"] = 0
        jr = pd.read_sql("select max(matched_id) from id_match where  id_type=1", engine_base)
        maxjr = jr.iloc[0, 0]
        m = int(maxjr.replace("JR", ""))
        new_fund["fund_id"] = generate_id(m + 1, len(new_fund))
        re = new1.append(new_fund)
        re.rename(columns={"fund_id": "matched_id"}, inplace=True)
        return re


source_fundaccount = '010002'
source_private = '010003'
source_securities = '010004'
source_futures = '010005'

from name.match_port import match_port, init_2_f


def get_name(df1):
    df_info = init_2_f()[0]
    try:
        df1.rename(columns={"source_id": "fund_id"}, inplace=True)
    except BaseException:
        pass
    else:
        pass
    a = match_port(df1, df_info, len(df1), len(df_info), d1_name='test_name', re_meth=1)
    if isinstance(a, float):
        print('没有匹配到x_info')
        df1.rename(columns={"fund_id": "source_id"}, inplace=True)
        df1["fund_id_get"] = None
        df1["fund_id"] = None

        return df1


    else:
        a.columns = ["match_target", "source_id", "base_name", "fund_id"]
        b = a.iloc[:, [1, 3]]
        dict1 = {key: value for key, value in zip(b["source_id"], b["fund_id"])}
        df1["fund_id_get"] = df1["fund_id"].apply(lambda x: dict1.get(x))
        df1.rename(columns={"fund_id": "source_id"}, inplace=True)
        return df1


# fund_fundaccount1 = df_fundaccount1()
# df1 = get_name(fund_fundaccount1)
# over1 = id_match(df1, source_fundaccount)
# to_sql("id_match", engine_base, over1, type="update")
#
# fund_private1 = df_private1()
# df2 = get_name(fund_private1)
# over2 = id_match(df2, source_private)
# to_sql("id_match", engine_base, over2, type="update")
#
# fund_securities1 = df_securities1()
# df3 = get_name(fund_securities1)
# over3 = id_match(df3, source_securities)
# to_sql("id_match", engine_base, over3, type="update")
# #
# fund_futures = df_futures()
# df4 = get_name(fund_futures)
# over4 = id_match(df4, source_futures)
# to_sql("id_match", engine_base, over4, type="update")


df020001 = df_020001()
df5 = get_name(df020001)
over5 = id_match(df5, '020001')


#
# df020002 = df_020002()
# df6 = get_name(df020002)
# over6 = id_match(df6, '020002')