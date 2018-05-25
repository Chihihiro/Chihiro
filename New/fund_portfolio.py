import pandas as pd
import sys
import time
import datetime
import numpy as np
from sqlalchemy import create_engine
from iosjk import *

engine_base = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),
    connect_args={"charset": "utf8"}, echo=True, )



def to_list(df):
    a = np.array(df)  # np.ndarray()
    vv = a.tolist()  # list
    return vv





def get_po_dict():
    df = pd.read_sql("SELECT subfund_id,fund_id FROM base.`fund_portfolio`", engine_base)
    df2 = df.iloc[:, [0, 1]]
    list = to_list(df2)
    po_dict = dict(list)
    return po_dict



po_dict = get_po_dict()



df = pd.read_sql("SELECT * FROM( \
            SELECT fp.subfund_id,fp.subfund_name,fp.fund_id,fp.fund_name, \
            ftp.type_code AS subfund_code,ftp.type_name AS subfund_code_name, \
            ftp2.type_code AS fund_code, ftp2.type_name AS fund_code_name \
            FROM base.`fund_portfolio` AS fp \
            JOIN base.fund_type_mapping as ftp \
            ON fp.subfund_id = ftp.fund_id \
            JOIN base.fund_type_mapping as ftp2 \
            ON fp.fund_id = ftp2.fund_id \
            WHERE ftp.flag =1 AND ftp2.flag =1 AND ftp.typestandard_code = '602' \
            AND fp.portfolio_type in (1,3,4) \
            AND ftp2.typestandard_code = '602' \
            AND ftp.type_code <> ftp2.type_code) as h \
            UNION( \
            SELECT fp.subfund_id,fp.subfund_name,fp.fund_id,fp.fund_name, \
            ftp.type_code AS subfund_code,ftp.type_name AS subfund_code_name, \
            ftp2.type_code AS fund_code, ftp2.type_name AS fund_code_name \
            FROM base.`fund_portfolio` AS fp \
            JOIN base.fund_type_mapping as ftp \
            ON fp.subfund_id = ftp.fund_id \
            JOIN base.fund_type_mapping as ftp2 \
            ON fp.fund_id = ftp2.fund_id \
            WHERE ftp.flag =1 AND ftp2.flag =1 AND ftp.typestandard_code = '603' \
            AND fp.portfolio_type in (1,3,4) \
            AND ftp2.typestandard_code = '603' \
            AND ftp.type_code <> ftp2.type_code) UNION( \
            SELECT fp.subfund_id,fp.subfund_name,fp.fund_id,fp.fund_name, \
            ftp.type_code AS subfund_code,ftp.type_name AS subfund_code_name, \
            ftp2.type_code AS fund_code, ftp2.type_name AS fund_code_name \
            FROM base.`fund_portfolio` AS fp \
            JOIN base.fund_type_mapping as ftp \
            ON fp.subfund_id = ftp.fund_id \
            JOIN base.fund_type_mapping as ftp2 \
            ON fp.fund_id = ftp2.fund_id \
            WHERE ftp.flag =1 AND ftp2.flag =1 AND ftp.typestandard_code = '604' \
            AND ftp2.typestandard_code = '604' \
            AND fp.portfolio_type in (1,3,4) \
            AND ftp.type_code <> ftp2.type_code)  \
            ", engine_base)


df2 = pd.read_csv('C:\\Users\\63220\Desktop\\Pycharm测试201805151724.csv')

f = open('C:\\Users\\63220\Desktop\\Pycharm测试201805151724.csv')
res = pd.read_csv(f)












