from engine import *
from DataAPI import *

import pandas as pd
import tushare as ts

df = ts.get_hist_data('600848')  # 一次性获取全部日k线数据

data = df.groupby(["index_id", "date"]).last()["value"].unstack().T.reset_index()

now = '2014-01-01'
before_day = '1990-12-19'
sql = "SELECT statistic_date,hs300,ssia,sse50,csi500 FROM base.market_index where statistic_date  BETWEEN '1990-12-19' and '2014-01-01' "
df = pd.read_sql(sql, engine_base)
df.rename(columns={"statistic_date": "date", "hs300": "000300.CSI", "ssia": "000002.CSI", "sse50": "000016.CSI",
                   "csi500": "000905.CSI"}, inplace=True)
df1 = df.T
df1 = df.set_index("date").reset_index()
df1 = df.set_index(["000300.CSI", "000002.CSI", "000016.CSI", "000905.CSI"]).reset_index()
df1 = df.set_index("date")
data = df1.unstack().reset_index()
data.columns = ["index_id", "date", "value"]
dataframe = data.dropna()

engine_base_finance = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base_finance', ),
    connect_args={"charset": "utf8"}, echo=False, )
from utils.database import config as cfg, io, sqlfactory as sf

engines = cfg.load_engine()
engine_base, engine_crawl_private = engines["2Gb"], engines["2Gcpri"]
io.to_sql("base_finance.index_value", engine_base, dataframe, type="update")

# #
# def main():
#     s1 = StreamsMain.stream_020002()
#     s1.dataframe
#     s1.flow(1)
#     print(a)
#     s1.flow(2)
#     b["purchase_range"].dropna()
