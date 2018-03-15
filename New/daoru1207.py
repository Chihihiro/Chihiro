import pandas as pd
from iosjk import to_sql
from sqlalchemy import create_engine
import numpy as np


JR=pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard",engine_base)

bb = np.array(JR)#np.ndarray()
vv=bb.tolist()#list


for i in vv:
    jr=i[0]
    nv=pd.read_sql("select fund_id,fund_name,statistic_date,nav,added_nav FROM fund_nv_data_standard where fund_id like '{}'".format(jr),engine_base)
    to_sql("fund_nv_data_standard", engine5, nv, type="update")
