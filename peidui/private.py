from engine import *
# def col_chi(userstr):
#     """
#     筛选字符中的中文字符，英文字符与阿拉伯数字 需要import re
#     Args:
#     userstr - str
#
#     Returns:
#     new_str - str
#     """
#     pattern = re.compile(r'[\u4e00-\u9fa5]|\d|[A-Z]|[a-z]|"信托"')
#     new_str = []
#     for i in range(len(userstr)):
#         if pattern.match(userstr[i]):
#             new_str.append(userstr[i])
#         else:
#             new_str.append('')
#     new_str = "".join(new_str)
#     return new_str
#
#
# def del_space(list_in):
#     """
#     将list中的空格元素删去
#     Args:
#     list_in - list
#
#     Returns:
#     list_out - list
#     """
#     if len(list_in) == 0:
#         return list_in
#     len_list = len(list_in)
#     for i in range(len_list - 1, -1, -1):
#         if list_in[i] == " ":
#             del list_in[i]
#     list_out = []
#     list_out[:] = list_in
#     return list_out
#
#
# def chinese2digits(uchars_chinese):
#     """
#     将输入的中文数字转化为阿拉伯数字
#     Args:
#     userstr - str
#
#     Returns:
#     total - int
#     """
#     total = 0
#     r = 1  # 表示单位：个十百千...
#     is_find = 0  # 判断是否为标准输入
#     common_used_numerals = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
#                             '十': 10, '百': 100, }  # 筛选数字字典
#     for i in range(len(uchars_chinese) - 1, -1, -1):
#         if common_used_numerals.get(uchars_chinese[i]) == '10':
#             is_find = 1
#             break
#     if len(uchars_chinese) >= 2 and is_find == 0:  # 不是标准输入
#         j = 1  # 十进制位数
#         for i in range(len(uchars_chinese) - 1, -1, -1):
#             val = common_used_numerals.get(uchars_chinese[i])
#             total = total + j * val
#             j = 10 * j
#         return total
#
#     for i in range(len(uchars_chinese) - 1, -1, -1):
#         val = common_used_numerals.get(uchars_chinese[i])
#         if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
#             if val > r:
#                 r = val
#                 total = total + val
#             else:
#                 r = r * val
#                 # total =total + r * x
#         elif val >= 10:
#             if val > r:
#                 r = val
#             else:
#                 r = r * val
#         else:
#             total = total + r * val
#     return total
#
#
# def sub_chi2num(userstr):
#     """
#     将输入的基金名称找出数字部分使用chines2digits函数将其替换并返回新的基金名称
#     Args:
#     userstr - str
#
#     Returns:
#     res_str - str
#     """
#     common_used_numerals = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
#                             '十': 10, '百': 100, }  # 筛选数字字典
#     if len(userstr) == 0:
#         return userstr
#     is_find = 0  # 判断是否找到数字
#     first = []  # 包含数字的字符首位
#     last = []  # 包含数字的字符末尾
#     str_list = list(userstr)
#     get_num = []  # 包含转换后的数字列表
#     nstr = len(userstr)
#     for i in range(nstr):
#         if is_find == 0:
#             if common_used_numerals.get(userstr[i]):
#                 is_find = 1
#                 first.append(i)
#         else:
#             if not common_used_numerals.get(userstr[i]):
#                 is_find = 0
#                 last.append(i - 1)
#         if i == len(userstr) - 1 and is_find == 1:  # 循环即将结束后还未找到非数字，则必然是该中文数字的最后一位
#             last.append(i)
#     nfirst = len(first)
#     for i in range(nfirst):
#         if last[i] <= nstr:
#             get_num.append(chinese2digits(userstr[first[i]:last[i] + 1]))
#         else:
#             get_num.append(chinese2digits(userstr[first[i]:]))
#     for i in range(nfirst - 1, -1, -1):
#         for j in range(last[i], first[i] - 1, -1):
#             del str_list[j]
#         str_list.insert(first[i], str(get_num[i]))
#     res_str = "".join(str_list)
#     return res_str


def df_fundaccount():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name_amac,reg_code_amac \
     FROM x_fund_info_fundaccount WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010002' and is_used=1)\
    and entry_time>'2017-11-19' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id;", engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_fundaccount


def df_private():
    df_private = pd.read_sql("SELECT * FROM (SELECT  fund_id \
    , fund_name_amac,reg_code_amac \
    FROM x_fund_info_private WHERE fund_id not in  \
    (SELECT source_id FROM base.id_match WHERE source='010003' and is_used=1) \
    and entry_time>'2017-11-20' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_private.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    # df_private["private_id"]=df_private["private_id"].apply(lambda x: 'ID:'+str(x))
    return df_private


def df_securities():
    df_securities = pd.read_sql("SELECT * FROM (SELECT   fund_id \
    , fund_name_amac,reg_code_amac FROM x_fund_info_securities WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010004' and is_used=1)  \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T-- securities \
    GROUP  BY T.fund_id;", engine_crawl_private)
    df_securities.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_securities


def df_futures():
    df_futures = pd.read_sql("SELECT * FROM (SELECT  fund_id, fund_name_amac,reg_code_amac \
     FROM x_fund_info_futures WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010005' and is_used=1) \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T -- futures \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_futures.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_futures


def df_haomai():
    engine_crawl_private.execute(
        "UPDATE d_fund_info set is_used=0  WHERE fund_id in (select a.fund_id from (select DISTINCT fund_id FROM d_fund_info where is_used=0) as a)")
    df_haomai = pd.read_sql("SELECT * FROM (SELECT fund_id,\
    fund_full_name FROM d_fund_info WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020001')\
    and source_id = '020001' \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id", engine_crawl_private)
    df_haomai.rename(columns={"fund_id": "haomai_id", "fund_full_name": "test_name"}, inplace=True)
    return df_haomai




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


def test2(private_null):
    if private_null.empty:
        print("米有匹配到")
        c = []
        d = []
        return c, d
    else:
        print("have")
        private_null["fund_id"] = private_null["test_name"].apply(lambda x: dict1.get(x))
        no = private_null.fillna("空")
        c = no[no['fund_id'] != '空']
        d = no[no['fund_id'] == '空']
        return c, d


def test1(private):
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
    fund_info["fund_id"] = fund_info["reg_code_amac"].apply(lambda x: dict.get(x))
    private = fund_info.loc[fund_info["fund_id"].notnull()]
    private_null = fund_info.loc[fund_info["fund_id"].isnull()]
    t1 = test1(private)
    t2 = test2(private_null)
    a = t1[0]
    b = t1[1]
    c = t2[0]
    d = t2[1]

    # result = pd.merge(a, c, how='outer')
    # result2 = pd.merge(b, d, how='outer')

    result = re_len(a, c)
    result2 = re_len(b, d)

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

from name.match_port import *
source_fundaccount = '010002'
source_private = '010003'
source_securities = '010004'
source_futures = '010005'

def get_name(df1):
    df_info = init_2_f()[0]
    df1.rename(columns={"source_id":"fund_id"},inplace=True)
    a=match_port(df1,df_info,len(df1),len(df_info),d1_name='test_name',re_meth=1)
    a.columns =["match_target","source_id","base_name","fund_id"]
    b=a.iloc[:,[1,3]]
    dict1 = {key: value for key, value in zip(b["source_id"], b["fund_id"])}
    df1["fund_id_get"]=df1["fund_id"].apply(lambda x: dict1.get(x))
    df1.rename(columns={"fund_id":"source_id"},inplace=True)
    return df1



fund_fundaccount = df_fundaccount()
over1 = id_match(fund_fundaccount, source_fundaccount)
to_sql("id_match", engine_base, over1, type="update")  # ignore



fund_private = df_private()
over2 = id_match(fund_private, source_private)
to_sql("id_match", engine_base, over2, type="update")  # ignore



fund_securities = df_securities()
over3 = id_match(fund_securities, source_securities)
to_sql("id_match", engine_base, over3, type="update")  # ignore

fund_futures = df_futures()
over4 = id_match(fund_futures, source_futures)
to_sql("id_match", engine_base, over4, type="update")  # ignore









