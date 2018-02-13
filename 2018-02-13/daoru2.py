from engine import *



df=pd.read_sql("SELECT * FROM (SELECT matched_id,source,source_id FROM base.id_match WHERE id_type=3 and source in ('020001','020002') and is_used=1) as t \
LEFT JOIN (select MAX(version),org_name,person_id,person_name from d_person_info GROUP BY person_id) as p ON t.source_id=p.person_id",engine_crawl_private)



df["org_name"]=df["org_name"].apply(lambda x: x.strip())


df["person_name"]=df["person_name"].apply(lambda x: x.strip())




df=pd.read_sql("SELECT * FROM `d_person_info` t1 \
JOIN (SELECT person_id, MAX(version) v FROM d_person_info GROUP BY person_id) t2 \
ON t1.person_id = t2.person_id AND t1.version = t2.v \
where t1.person_id not in (SELECT source_id FROM base.id_match where id_type=3 and source in ('020001','020002'))",engine_crawl_private)
df["person_name"]=df["person_name"].apply(lambda x: x.strip())