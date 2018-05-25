from engine import *


sql1 = "SELECT org_id,major_shareholder FROM base_test.`org_info_20180327` WHERE major_shareholder is not NULL"


df = pd.read_sql(sql1, engine_base_test)
to_sql("y_org_description",engine_crawl_private,df,type='update')



sql2 = "SELECT org_id,shareholder_structure FROM base_test.`org_info_20180327` WHERE shareholder_structure is not NULL"

df2 = pd.read_sql(sql2, engine_base_test)
df2["shareholder_structure"] = df2["shareholder_structure"].apply(lambda x: sub_wrong_to_none(x))




sql3 = "SELECT org_id,prize FROM crawl_private.`y_org_description` WHERE prize is not NULL"


df3["prize"] = df3["prize"].apply(lambda x: None if "公众号" in x else x)