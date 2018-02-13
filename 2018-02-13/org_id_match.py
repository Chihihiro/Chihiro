from engine import *


df=pd.read_sql("SELECT org_id from (SELECT  MAX(version),org_id FROM x_org_info WHERE  org_id NOT in (select org_id FROM base.org_info)  GROUP BY org_id) as t ORDER BY org_id",engine_crawl_private)


def org_id_match():
    org=df["org_id"]
    df["org_ID"]=org
    df["match_type"]=5
    df.rename(columns={"org_id":"source_org_ID"},inplace=True)
    inp=df
    return inp


dataframe=org_id_match()

to_sql("org_id_match", engine_base, dataframe, type="update")