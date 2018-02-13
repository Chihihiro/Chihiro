import pandas as pd
from History.iosjk import to_sql
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171),connect_args={"charset": "utf8"}, echo=True, )
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root','','localhost',3306,'test', ), connect_args={"charset": "utf8"},echo=True,)
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'base', ),connect_args={"charset": "utf8"}, echo=True, )
engine4 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'crawl_private', ),connect_args={"charset": "utf8"}, echo=True, )


df=pd.read_excel('C:\\Users\\63220\\Desktop\\fund_id1225.xls')


# qq=pd.read_sql("select source_ID from fund_id_match where fund_id in ('JR156974',	'JR156975',	'JR156976',	'JR156977',	'JR156978',	'JR156979',	'JR156980',	'JR156981',	'JR156982',	'JR156983',	'JR156984',	'JR156985',	'JR156986',	'JR156987',	'JR156988',	'JR156989',	'JR156990',	'JR156991',	'JR156992',	'JR156993',	'JR156994',	'JR156995',	'JR156996',	'JR156997',	'JR156998',	'JR156999',	'JR157000',	'JR157001',	'JR157002',	'JR157003',	'JR157004',	'JR157005',	'JR157006',	'JR157007',	'JR157008',	'JR157009',	'JR157010',	'JR157011',	'JR157012',	'JR157013',	'JR157014',	'JR157015',	'JR157016',	'JR157017',	'JR157018',	'JR157019',	'JR157020',	'JR157021',	'JR157022',	'JR157023',	'JR157024',	'JR157025',	'JR157026',	'JR157027',	'JR157028',	'JR157029',	'JR157030',	'JR157031',	'JR157032',	'JR157033',	'JR157034',	'JR157035',	'JR157036',	'JR157037',	'JR157038',	'JR157039',	'JR157040',	'JR157041',	'JR157042',	'JR157043',	'JR157044',	'JR157045',	'JR157046',	'JR157047',	'JR157048',	'JR157049',	'JR157050',	'JR157051',	'JR157052',	'JR157053',	'JR157054',	'JR157055',	'JR157056',	'JR157057',	'JR157058',	'JR157059',	'JR157060',	'JR157061',	'JR157062',	'JR157063',	'JR157064',	'JR157065',	'JR157066',	'JR157067',	'JR157068',	'JR157069',	'JR157070',	'JR157071',	'JR157072',	'JR157073',	'JR157074',	'JR157075',	'JR157076',	'JR157077',	'JR157078',	'JR157079',	'JR157080',	'JR157081',	'JR157082',	'JR157083',	'JR157084',	'JR157085',	'JR157086',	'JR157087',	'JR157088',	'JR157089',	'JR157090',	'JR157091',	'JR157092',	'JR157093',	'JR157094',	'JR157095')",engine3)
source_ID=df["source"].tolist()
# print(source_ID)
a = ','.join(source_ID)
print(a)

df_cl=pd.read_sql("SELECT	  ba.fund_id,xoi.fund_id as cl_fund_id	,xoi. fund_name	,xoi. fund_full_name	,xoi. reg_code	,xoi. reg_time	,xoi. fund_status	,xoi. fund_manager	,xoi. fund_administrion	,xoi. locked_time_limit	,xoi. open_date	,xoi. end_date	,xoi. min_purchase_amount	,xoi. min_append_amount	,xoi. fee_subscription	,xoi. fee_redeem	,xoi. fee_pay	,xoi. fee_manage	,xoi. fee_trust	,xoi. fund_manager_nominal	,xoi. fund_custodian	,xoi. fund_stockbroker	,xoi. fund_futurebroker	,xoi. foundation_date	,xoi.fund_consultant	FROM	crawl_private.d_fund_info AS xoi	JOIN (	SELECT	fund_id,	MAX(version) max_version	FROM	crawl_private.d_fund_info	GROUP BY	fund_id	) tb_mv ON xoi.fund_id = tb_mv.fund_id	AND xoi.version = tb_mv.max_version LEFT JOIN base.fund_id_match as ba on ba.source_ID=xoi.fund_id	where xoi.source_id=020001 AND ba.match_type=3  and xoi.fund_id in ({})".format(a),engine)




del df_cl['cl_fund_id']
df_cl.rename(columns={'fund_manager_nominal':'fund_manager_nominal_', 'fund_manager':'fund_member', 'fund_consultant':'fund_manager','fund_name':'fund_name_1'}, inplace = True)

# gg=df_cl["fund_id","fund_status"].tolist()
# del df_cl["fund_status"]
# print(gg)
#
STATUS_MAP = {
    "正常": "运行中",
}
df_cl["fund_status"] = df_cl["fund_status"].apply(lambda x: STATUS_MAP.get(x))

cols_used = ["fund_id", "fund_name_1",]
result = df_cl[cols_used]
#
# aa=df_cl["fund_status"].tolist()
# del df_cl["fund_status"]
# for i in range(len(aa)):
#     if aa[i] == '正常':
#         aa[i] = '运行中'
#
# print(aa)
# # bb=df_cl["fund_id"].tolist()
# # BB=pd.DataFrame(bb)
#
# AA=pd.DataFrame(aa)
# AA.columns = ["fund_status"]
# print(AA)
# print(df_cl)
#
# # frames=[BB,AA]
# #
# # result=pd.concat(frames)
# #
# # print(result)
# # #
# # result.to_csv('C:\\Users\\63220\\Desktop\\11-19.csv')
# # df_cl.to_csv('C:\\Users\\63220\\Desktop\\11-17.csv')
to_sql("fund_info", engine3, df_cl, type="update")