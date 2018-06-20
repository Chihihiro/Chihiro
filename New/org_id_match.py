from engine import *

df = pd.read_sql(
    "SELECT org_id from (SELECT  MAX(version),org_id FROM x_org_info WHERE  org_id NOT in (select org_id FROM base.org_info)  GROUP BY org_id) as t ORDER BY org_id",
    engine_crawl_private)


df.rename(columns={"org_id": "matched_id"}, inplace=True)
df["source_id"] = df["matched_id"]
df["id_type"] = 2
df["source"] = "010001"
df["is_used"] = 1
df["is_del"] = 0



dataframe=org_id_match()

# to_sql("org_id_match", engine_base, dataframe, type="update")