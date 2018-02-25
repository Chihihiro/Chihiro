from sqlalchemy import create_engine
from iosjk import *
from engine import *


df=pd.read_sql("SELECT a.fund_id,b.fund_name,b.statistic_date,b.nav,b.added_nav,b.adjusted_nav FROM base.fund_id_match AS a  LEFT JOIN crawl_private.d_fund_nv as b on a.source_ID=b.fund_id WHERE b.fund_id in (SELECT DISTINCT fund_id FROM crawl_private.d_fund_nv )",engine)

df.rename(columns=lambda x:x.replace('adjusted_nav','swanav'), inplace=True)
df['source_code'] ='3'
df['source'] ='第三方'
df['data_source'] ='13'
df['data_source_name'] ='wind'
print(df)
dataframe=df
to_sql("fund_nv_data_source", engine_base, dataframe, type="update")


# df=pd.read_sql("SELECT MAX(a.fund_id )as source_ID FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM (SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE 'ZB%%' GROUP BY fund_ID) AS a WHERE a.t > 1) GROUP  BY b.fund_ID",engine3)
#
# print(df)
# dds=df["source_ID"].tolist()
# ','.join(dds)
# print(dds)
# # dd=()
#
#
# df2=pd.read_sql("UPDATE fund_info_privateSET flag=1 WHERE fund_id in ({})".format(dds),engine2)




    # 'SELECT a.fund_id FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM(SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE '%ZB%' GROUP BY fund_ID) AS a WHERE a.t > 1) ORDER BY b.fund_ID ) as p where fund_id NOT in (SELECT MAX(a.fund_id )as source_ID FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM (SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE '%ZB%' GROUP BY fund_ID ) AS a WHERE a.t > 1) GROUP  BY b.fund_ID)
