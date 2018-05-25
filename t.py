import pandas as pd
from sqlalchemy import create_engine

conn_string = "mysql+pymysql://sm01:x6B28Vz9@182.254.128.241:8612/app_mutual"
eg_gbk = create_engine(conn_string, connect_args={"charset": "gbk"}, echo=True)
eg_utf8 = create_engine(conn_string, connect_args={"charset": "utf8"}, echo=True)
eg_default = create_engine(conn_string, echo=True)

eg_write = create_engine("mysql+pymysql://root:@localhost:3306/test_gbk", echo=True)
eg_write2 = create_engine("mysql+pymysql://root:@localhost:3306/test_gbk", connect_args={"charset": "utf8"}, echo=True)
eg_write3 = create_engine("mysql+pymysql://root:@localhost:3306/test_gbk", connect_args={"charset": "gbk"}, echo=True)

for e in (eg_gbk, eg_utf8, eg_default, eg_write):
    try:
        e.connect()
    except Exception as err:
        print(err)


sql = "SELECT * FROM fund_info"

# read in different encoding
d1 = pd.read_sql(sql, eg_gbk)
d2 = pd.read_sql(sql, eg_utf8)
d3 = pd.read_sql(sql, eg_default)

# write
d2.to_sql('fund_info', eg_write, schema='test_gbk', if_exists='append', index=False)
d2.to_sql('fund_info', eg_write2, schema='test_gbk', if_exists='append', index=False)
d2.to_sql('fund_info', eg_write3, schema='test_gbk', if_exists='append', index=False)


# read again
d = pd.read_sql("SELECT * FROM test_gbk.fund_info", eg_write)
d = pd.read_sql("SELECT * FROM test_gbk.fund_info", eg_write2)
d = pd.read_sql("SELECT * FROM test_gbk.fund_info", eg_write3)


d2.to_sql()
