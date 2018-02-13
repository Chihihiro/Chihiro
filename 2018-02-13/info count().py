from engine import *

import re


def sub_wrong_to_none(x):
    try:
        s = re.sub("\s|-| ", "", x)
        if s == "":
            return None
        else:
            return s
    except:
        return None


def check_dataframe(dataframe, columns_to_check):
    dataframe[columns_to_check] = dataframe[columns_to_check].applymap(lambda x: sub_wrong_to_none(x))

x=['currency',	'is_reg',	'is_private',	'fund_type_strategy',	'fund_type_investment_way',	'fund_member',	'fund_manager',	'fund_consultant',	'fund_manager_nominal',	'fund_manager_nominal_',	'fund_custodian',	'fund_administrion',	'fund_stockbroker',	'fund_futurebroker',	'fund_status',	'end_date',	'fund_time_limit',	'is_abnormal_liquidation',	'liquidation_cause',	'fund_type_issuance',	'fund_type_structure',	'structure_hierarchy',	'is_umbrella_fund',	'manage_type',	'is_deposit',	'region',	'open_date',	'duration',	'locked_time_limit',	'min_purchase_amount',	'min_append_amount',	'min_append_amount_remark',	'fee_subscription',	'fee_redeem',	'fee_manage',	'fee_trust',	'fee_manage_remark',	'fee_pay',	'fee_pay_remark',	'data_freq',	'init_nav',	'init_total_asset']


df=pd.read_sql("select * from fund_info",engine_base)
info=df.iloc[:,range(50)]
check_dataframe(info, x)

del info["fund_consultant"]
df.drop('fund_consultant',axis=1, inplace=True)
info["currency_type"]=info["currency"]
info.rename(columns={"fund_manager":"fund_consultant"},inplace=True)

info.rename(columns={"fund_member":"fund_manager","locked_time_limit":"limit_time"},inplace=True)
info["limit_date"]=info["limit_time"]
c=info.count()

y=len(info["fund_id"])
df_a = c.reset_index()
df_a.columns=["字段名字","非空字段数量"]
df_a["空字段数量"]=y-df_a["非空字段数量"]
df_a["非空百分比"]=df_a["非空字段数量"]/y


df2=pd.read_sql("select * from fund_info_test",engine_base)
test=df2.iloc[:,range(50)]
c2=test.count()
df_b = c2.reset_index()
u=len(test["fund_id"])

df_b.columns=["字段名字","test非空字段数量"]
df_b["test空字段"]=u-df_b["test非空字段数量"]
df_b["test非空百分比"]=df_b["test非空字段数量"]/u

df_all=df_b.merge(df_a,on="字段名字",how="left")

df_all["改进后"]=df_all["test非空百分比"]-df_all["非空百分比"]
df_all["改进后"]=df_all["改进后"].apply(lambda x: '%.2f' % (x*100))
df_all["改进后"]=df_all["改进后"].apply(lambda x: str(x)+'%')

# df_all.fillna(0)
df_all["非空字段数量"] = df_all["非空字段数量"].apply(lambda x: float('%.0f' % x))
df_all["空字段数量"] = df_all["空字段数量"].apply(lambda x: '%.0f' % x)
# df_all["非空字段数量"]=df_all["非空字段数量"].apply(lambda x: int(x)if x is not None else None)
# df_all["两表相差数量"]=df_all["test非空字段数量"]-df_all["非空字段数量"]
# df_all["两表相差数量"]=df_all["两表相差数量"].apply(lambda x: '%.0f' % x if x is not None else None)
# df_all["test非空字段数量"]=df_all["test非空字段数量"].apply(lambda x: '%.0f' % x if x is not None else None)

df_all["test非空百分比"] = df_all["test非空百分比"].apply(lambda x: '%.2f' % (x*100))
df_all["test非空百分比"] = df_all["test非空百分比"].apply(lambda x: str(x)+'%')

df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: '%.2f' % (x*100))
df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: str(x)+'%')








