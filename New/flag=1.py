from sqlalchemy import create_engine
from History.iosjk import *

# df=pd.read_sql("SELECT MAX(a.fund_id )as source_ID FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM (SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE 'ZB%%' GROUP BY fund_ID) AS a WHERE a.t > 1) GROUP  BY b.fund_ID",engine3)
#
# print(df)
# dds=df["source_ID"].tolist()
# print(dds)
# pp=','.join(dds)
# print(pp)
#
# cs=pd.read_sql("SELECT a.fund_id FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM(SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE 'ZB%%' GROUP BY fund_ID) AS a WHERE a.t > 1) ORDER BY b.fund_ID ) as p where fund_id NOT in (SELECT MAX(a.fund_id )as source_ID FROM fund_info_private AS a INNER JOIN fund_id_match AS b ON a.fund_id = b.source_ID WHERE b.match_type = 5 AND b.fund_ID IN (SELECT fund_ID FROM (SELECT fund_ID,count(fund_ID) AS t FROM fund_id_match WHERE match_type = 5 AND source_ID LIKE 'ZB%%' GROUP BY fund_ID ) AS a WHERE a.t > 1) GROUP  BY b.fund_ID)",engine3)
# ccs=cs["source_ID"].tolist()
# little=','.join(ccs)



# engine2.execute("update t_doubantop set name='廖文斌的救赎' WHERE num=1",engine2)
update_test('update t_doubantop set name="廖文斌的救赎kaka" WHERE num=1')
exit()


