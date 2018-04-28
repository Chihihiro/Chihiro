from engine import *

df = pd.read_sql("SELECT * FROM ( \
    select * from ( \
    SELECT matched_id,source from base.id_match where id_type=1 and source not in ('010001','010002','010003','010004','010005') and is_used=1 \
    and matched_id not in (select DISTINCT pk from config_private.sync_source where source_id not in ('010001','010002','010003','010004','010005') and is_used=1) \
    ) as c GROUP BY c.matched_id  \
    ) as a \
    LEFT JOIN (select fund_id,COUNT(fund_id) as countnum from base.fund_nv_data_source_copy2 GROUP BY fund_id) as b ON a.matched_id=b.fund_id \
    WHERE b.countnum >1", engine_crawl_private)



a = df.iloc[:,[0,1]]

a.rename(columns={"matched_id": "pk", "source": "source_id"}, inplace=True)

a["target_table"] = "fund_nv_data_standard"

a["priority"] = 7
a["is_used"] = 1
print(a)
to_sql("sync_source", engine_config_private, a, type="update")

#
# sql1 = "SELECT * FROM ( \
#         SELECT pk,source_id FROM config_private.sync_source WHERE is_used=1 AND source_id NOT in ('05','04','03') \
#         ) as b \
#         LEFT JOIN ( \
#         SELECT matched_id,source,source_id from base.id_match where id_type=1 and source not in ('010001','010002','010003','010004','010005') and is_used=1  \
#         ) as a ON a.matched_id=b.pk AND a.source=b.source_id"
#
#
# sql2 ="SELECT matched_id,source,source_id from base.id_match where id_type=1 and source not in ('010001','010002','010003','010004','010005') and is_used=1 "
#
#
#
# df=pd.read_sql(sql1,engine_config_private)