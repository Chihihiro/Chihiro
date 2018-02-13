from History.iosjk import to_sql
from History.engine import *
import time

now = time.strftime("%Y-%m-%d")



def to_ku():
    df_limit = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date FROM fund_nv_data_standard WHERE statistic_date > '2017-10-01'\
                        ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ORDER BY RAND() LIMIT 1000",engine_base)
    df_limit["version"]=now
    df_limit.rename(columns={"statistic_date": "standard"}, inplace=True)
    c=df_limit.iloc[:,[0,1,2,3]]
    to_sql("zhaoyang", engine5, c, type="update")
    df_JR = df_limit["fund_id"].tolist()
    JR = '\'' + "','".join(df_JR) + '\''
    full_name=pd.read_sql("select fund_id,fund_full_name from fund_info WHERE fund_id in ({})".format(JR),engine_base)
    full_name.rename(columns={"fund_full_name": "fund_name"}, inplace=True)
    full_name["version"]=now
    to_sql("zhaoyang", engine5, full_name, type="update")
    print("样本1000只固定")




def sanfang():
    df_limit = pd.read_sql("select fund_id,fund_name from zhaoyang WHERE version='{}'".format(now), engine5)
    df_JR = df_limit["fund_id"].tolist()
    JR = '\'' + "','".join(df_JR) + '\''
    # df_name = df_limit["fund_name"].tolist()
    df_source=pd.read_sql("SELECT * FROM (SELECT fund_id,statistic_date  FROM fund_nv_data_source WHERE \
     fund_id in ({}) ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ".format(JR),engine_base)
    df_source.rename(columns={"statistic_date": "nv_source"}, inplace=True)
    df_source["version"]=now
    to_sql("zhaoyang", engine5, df_source, type="update")


    df_source_copy2=pd.read_sql("SELECT * FROM (SELECT fund_id,statistic_date  FROM fund_nv_data_source_copy2 WHERE \
     fund_id in ({}) ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ".format(JR),engine_base)
    df_source_copy2.rename(columns={"statistic_date": "nv_source_copy2"}, inplace=True)
    df_source_copy2["version"]=now
    to_sql("zhaoyang", engine5, df_source_copy2, type="update")

    df_standard_copy2=pd.read_sql("SELECT * FROM (SELECT fund_id,statistic_date  FROM fund_nv_data_standard_copy2 WHERE \
     fund_id in ({}) ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ".format(JR),engine_base)
    df_standard_copy2.rename(columns={"statistic_date": "nv_standard_copy2"}, inplace=True)
    df_standard_copy2["version"]=now
    to_sql("zhaoyang", engine5, df_standard_copy2, type="update")


    df_jfz=pd.read_sql("SELECT * FROM (SELECT fund_id,statistic_date FROM fund_nv_data_source_copy2 WHERE source_id='020002'\
    and fund_id in ({}) ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ".format(JR),engine_base)
    df_jfz.rename(columns={"statistic_date": "jinfuzi"}, inplace=True)
    df_jfz["version"]=now
    to_sql("zhaoyang", engine5, df_jfz, type="update")


    df_haomai=pd.read_sql("SELECT * FROM (SELECT fund_id,statistic_date FROM fund_nv_data_source_copy2 WHERE source_id='020001'\
    and fund_id in ({}) ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id ".format(JR),engine_base)
    df_haomai.rename(columns={"statistic_date": "haomai"}, inplace=True)
    df_haomai["version"]=now
    to_sql("zhaoyang", engine5, df_haomai, type="update")
    print("三方日期导入")


def daochu():
    df = pd.read_sql("SELECT fund_id,fund_name,zhaoyang,standard,nv_source,nv_source_copy2,nv_standard_copy2,jinfuzi,haomai\
                        FROM zhaoyang WHERE  version = '{}' AND zhaoyang is NOT NULL ".format(now), engine5)

    df["differ_day"] = (df["zhaoyang"] - df["standard"])
    df["differ_day"] = df["differ_day"].apply(lambda x: x.days)
    aa = df[df.differ_day > 0]
    a = len(aa)
    # a=len(df["differ_day"]<0)
    df["standard慢的"] = None
    df.iloc[0, -1] = a
    bb = df[df.differ_day < 0]
    b = len(bb)
    df["standard提前更的"] = None
    df.iloc[0, -1] = b

    df["differ_day_copy2"] = (df["zhaoyang"] - df["nv_source_copy2"])
    df["differ_day_copy2"] = df["differ_day_copy2"].apply(lambda x: x.days)

    cc = df[df.differ_day_copy2 > 0]
    c = len(cc)
    # a=len(df["differ_day"]<0)
    df["copy2速度慢的"] = None
    df.iloc[0, -1] = c
    dd = df[df.differ_day_copy2 < 0]
    d = len(dd)
    df["copy2提前更的"] = None
    df.iloc[0, -1] = d

    df["differ_day_standard_copy2"] = (df["zhaoyang"] - df["nv_standard_copy2"])
    df["differ_day_standard_copy2"] = df["differ_day_standard_copy2"].apply(lambda x: x.days)
    ss = df[df.differ_day_standard_copy2 > 0]
    s = len(ss)
    # a=len(df["differ_day"]<0)
    df["standard_copy2速度慢的"] = None
    df.iloc[0, -1] = s
    dd = df[df.differ_day_standard_copy2 < 0]
    f = len(dd)
    df["standard_copy2提前更的"] = None
    df.iloc[0, -1] = f


    df["differ_day_source"] = (df["zhaoyang"] - df["nv_source"])
    df["differ_day_source"] = df["differ_day_source"].apply(lambda x: x.days)

    yy = df[df.differ_day_source > 0]
    y = len(yy)
    # a=len(df["differ_day"]<0)
    df["source速度慢的"] = None
    df.iloc[0, -1] = y
    zz = df[df.differ_day_source < 0]
    z = len(zz)
    df["source提前更的"] = None
    df.iloc[0, -1] = z





    now2 = time.strftime("%Y%m%d%H%M")
    df.to_excel("E:\\SVN目录\\数据部_\\质量管理\\净值质量跟踪\\私募报表\\私募净值追踪{}.xlsx".format(now2))
    print("导出完毕")

    #
    # book = xlsxwriter.Workbook("C:\\Users\\63220\\Desktop\\插入测试.xlsx")
    # sheet = book.add_worksheet('Sheet1')
    # sheet.insert_image('Q5', 'C:\\Users\\63220\\PycharmProjects\\QQX\\ceshi.png')
    #
    #
    #



#
to_ku()#"样本1000只固定"
sanfang()
#
# from zhaoyang import *

# daochu()
#
# writer = pd.ExcelWriter("C:\\Users\\63220\\Desktop\\插入测试.xlsx")
# df.to_excel(writer, 'Sheet1')
# writer.save()

