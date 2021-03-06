from WindPy import w as wind
from sqlalchemy import create_engine
import pandas as pd
import time
from iosjk import to_sql
from engine import *

now = time.strftime("%Y-%m-%d")
now2 = time.strftime("%Y%m%d%H%M")


def crawl_benchmark(id):
    wind.start()  # 启动wind
    all_id = str(id) + '.OF'
    tmp = wind.wsd(all_id, "NAV_date,nav,NAV_acc", "ED0D", now, "")  # 万得API用法
    df = pd.DataFrame(tmp.Data)
    df = df.T
    if df[0][0] == None:
        pass
    else:
        df.columns = ["statistic_date_wind", "nav_wind", "added_nav_wind"]
        df["statistic_date_wind"] = df["statistic_date_wind"].apply(lambda x: x.strftime('%Y-%m-%d'))
        df["fund_id"] = id
        df["version"] = now
        to_sql("fund_nv", engine5, df, type="update")
        print(df)


def pu_all():
    df = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv\
    where fund_id NOT IN ('A80001','A80002','A80003','A80004','B90001','B90002','BE0001','BE0002','BE0051','C60001')\
    ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id", engine_pu)
    df["version"] = now
    to_sql("fund_nv", engine5, df, type="update")
    return print("公募库最新提取到本地库")


def pu_all2():
    df = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date,return_10k,d7_return_a FROM fund_yield\
                    ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id", engine_pu)
    df["version"] = now
    df.rename(columns={"return_10k": "nav", "d7_return_a": "added_nav"}, inplace=True)
    to_sql("fund_nv", engine5, df, type="update")
    return print("公募库货币型提取到本地库")


def pu_all3():
    df = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name,statistic_date,return_10k,d7_return_a FROM fund_yield\
                    ORDER BY statistic_date DESC ) AS T GROUP  BY T.fund_id", engine_pu)
    df["version"] = now
    df.rename(columns={"return_10k": "nav", "d7_return_a": "added_nav"}, inplace=True)
    df2 = pd.read_sql("select fund_id,statistic_date from fund_nv where version='{}'".format(now), engine5)
    df2.rename(columns={"statistic_date": "y_date"}, inplace=True)
    DF = pd.merge(df, df2, how='left')
    dif1 = DF.loc[~DF["y_date"].notnull()]
    del dif1["y_date"]
    dif2 = DF.loc[DF["y_date"] < DF["statistic_date"]]
    del dif2["y_date"]
    to_sql("fund_nv", engine5, dif1, type="update")
    to_sql("fund_nv", engine5, dif2, type="update")
    return print("公募库货币型提取到本地库")


# --------------------------------------报表导出
def sub_wrong_to_none(x):
    s = re.sub("\s|-| |--|---|]|\[", "", x)
    if s == "":
        return None
    else:
        return s

def daochu():
    ff = pd.read_sql("SELECT fund_id from base_public.fund_info where fund_status = '终止'", engine_base_public)
    shanchu = sub_wrong_to_none(str(ff["fund_id"].tolist()))
    df = pd.read_sql("SELECT version,fund_id,fund_name,statistic_date,nav,added_nav,statistic_date_wind,nav_wind,added_nav_wind\
                    FROM fund_nv WHERE  version = '{}' AND fund_id NOT IN ({})".format(now, shanchu), engine5)

    df["differ_day"] = (df["statistic_date"] - df["statistic_date_wind"])
    df["differ_day"] = df["differ_day"].apply(lambda x: x.days)
    aa = df[df.differ_day < 0]
    a = len(aa)
    df["速度慢的基金数量"] = None
    df.iloc[0, -1] = a
    df["fund_id"] = df["fund_id"].apply(lambda x: str(x) + ".OF")

    bb = df[df.differ_day > 0]
    b = len(bb)
    df["提前更新数量"] = None
    df.iloc[0, -1] = b

    df.to_csv("E:\\SVN目录\\Data_work\\质量管理\\净值质量跟踪\\公募报表\\公募净值追踪{}.csv".format(now2))
    print("导出完毕")


def wind_pull():
    df = pd.read_excel("C:\\Users\\63220\\Desktop\\wind净值每日.xlsx")
    # df["fund_id"]=df["fund_id"].apply(lambda x: "%06d" % x)
    df["nav_wind"] = df["nav_wind"].apply(lambda x: '%.4f' % x)
    df["added_nav_wind"] = df["added_nav_wind"].apply(lambda x: '%.4f' % x)
    df["version"] = now
    df2 = df[(df.nav_wind != '0.0000') & (df.added_nav_wind != '0.0000')]
    to_sql("fund_nv", engine5, df2, type="update")
    return print("wind公募全量提取到本地库")


def main():
    wind_pull()
    pu_all()
    pu_all3()
    daochu()
    print(now2)


if __name__ == "__main__":
    main()
