from sqlalchemy import Column, String, create_engine
import pandas as pd
import datetime,time
from iosjk import to_sql
import numpy as np
from engine import *


def del_fund_max(fund_id):
    engine_base.execute("DELETE from fund_nv_updata_source where fund_id='{a}';\
    DELETE from fund_nv_data_standard where fund_id='{a}';\
    DELETE from fund_type_mapping where fund_id='{a}';\
    DELETE from fund_type_source where fund_id='{a}';\
    DELETE from fund_org_mapping where fund_id='{a}';\
    DELETE from fund_manager_mapping where fund_id='{a}';\
    DELETE from fund_allocation_data where fund_id='{a}';\
    DELETE from fund_info where fund_id='{a}';\
    DELETE from fund_info_subsidiary where fund_id='{a}';\
    DELETE FROM fund_allocation_data WHERE fund_id='{a}' \
    DELETE FROM fund_asset_scale WHERE fund_id='{a}';\
    DELETE FROM fund_fee_data WHERE fund_id='{a}';\
    DELETE FROM fund_id_match WHERE fund_id='{a}';\
    DELETE FROM fund_info_aggregation WHERE fund_id='{a}';\
    DELETE FROM fund_nv_data_source WHERE fund_id='{a}';\
    UPDATE id_match  set is_used = 0 WHERE matched_id='{a}';\
    DELETE from fund_month_indicator where fund_id='{a}';\
    DELETE from fund_month_return where fund_id='{a}';\
    DELETE from fund_month_risk where fund_id='{a}';\
    DELETE from fund_month_risk2 where fund_id='{a}';\
    DELETE from fund_nv_standard_m where fund_id='{a}';\
    DELETE from fund_nv_standard_w where fund_id='{a}';\
    DELETE from fund_portfolio WHERE subfund_id='{a}';\
    DELETE from fund_security_data where fund_id='{a}';\
    DELETE from fund_subsidiary_month_index where fund_id='{a}';\
    DELETE from fund_subsidiary_month_index2 where fund_id='{a}';\
    DELETE from fund_subsidiary_month_index3 where fund_id='{a}';\
    DELETE from fund_subsidiary_weekly_index where fund_id='{a}';\
    DELETE from fund_subsidiary_weekly_index2 where fund_id='{a}';\
    DELETE from fund_subsidiary_weekly_index3 where fund_id='{a}';\
    DELETE from fund_weekly_indicator where fund_id='{a}';\
    DELETE from fund_weekly_return where fund_id='{a}';\
    DELETE from fund_weekly_risk where fund_id='{a}';\
    DELETE from fund_weekly_risk2 where fund_id='{a}'".format(a=fund_id))


def del_fund_min(fund_id):
    engine_base.execute("DELETE from fund_nv_updata_source where fund_id='{a}';\
    DELETE from fund_nv_data_standard where fund_id='{a}';\
    DELETE from fund_type_mapping where fund_id='{a}';\
    DELETE from fund_type_source where fund_id='{a}';\
    DELETE from fund_org_mapping where fund_id='{a}';\
    DELETE from fund_manager_mapping where fund_id='{a}';\
    DELETE from fund_allocation_data where fund_id='{a}';\
    DELETE from fund_info where fund_id='{a}';\
    DELETE from fund_info_subsidiary where fund_id='{a}';\
    DELETE FROM fund_id_match WHERE fund_id='{a}';\
    DELETE FROM fund_info_aggregation WHERE fund_id='{a}';\
    DELETE FROM fund_nv_data_source WHERE fund_id='{a}';\
    UPDATE id_match  set is_used,is_del=1 = 0 WHERE matched_id='{a}';".format(a=fund_id))
    engine_crawl_private.execute("DELETE FROM y_fund_nv where fund_id='{}'".format(fund_id))


def combine_fund(JR,D_JR):
    r1=pd.read_sql("select fund_ID,source_ID,match_type from fund_id_match where fund_id in ('{}','{}')".format(JR,D_JR),engine_base)
    r1["fund_ID"]=JR
    df=r1.drop_duplicates()
    to_sql("fund_id_match",engine_base,df,type="update")
    print("r1")
    r2=pd.read_sql("select fund_id,data_source,is_updata from fund_nv_updata_source where fund_id in ('{}','{}') AND is_updata=1".format(JR,D_JR),engine_base)
    r2["fund_id"]=JR
    df2=r2.drop_duplicates()
    to_sql("fund_nv_updata_source",engine_base,df2,type="update")
    print("r2")
    nv=pd.read_sql("select fund_id,statistic_date,data_source,data_source_name,source_code,source,nav,added_nav from fund_nv_data_source where fund_id='{}'".format(D_JR),engine_base)
    nv["fund_id"]= JR
    to_sql("fund_nv_data_source",engine_base,nv,type="update")
    print("nv")
    del_fund_min(D_JR)
    print("down")

def show_info(JR,D_JR):
    r = pd.read_sql("select * from fund_info where fund_id in ('{}','{}')".format(JR, D_JR), engine_base)
    print(r)






#
# D_JR="JR999558"
# # JR="JR044733"
# del_fund(D_JR)



D_JR="JR001050"
JR="JR057851"
r=pd.read_sql("select * from fund_info where fund_id in ('{}','{}')".format(JR,D_JR),engine_base)



combine_fund(JR,D_JR)




df = pd.read_excel('C:\\Users\\63220\Desktop\\D_JR.xls')
c=to_list(df)

for i in c:
    D_JR=i[0]
    JR=i[1]
    print(D_JR,JR)
    combine_fund(JR, D_JR)