from sqlalchemy import Column, String, create_engine
import pandas as pd
import numpy as np


def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv



def table_name():
    cc = pd.read_sql("show tables", engine_lijia)
    bb = np.array(cc)#np.ndarray()
    vv=bb.tolist()#list
    vv.remove(["id_match"])
    vv.remove(["id_match_copy"])
    tablenames=[]
    for q in vv:
        str = "".join(q)
        pp=pd.read_sql("show columns from {}".format(str),engine_lijia)
        li=pp["Field"]
        col = np.array(li)
        print(li)
        y="org_id"

        if y in col:
            tablenames.append(str)
        else:
            print("米有")

    return tablenames


table=table_name()
df=pd.read_sql("select matched_id from id_match WHERE is_del=1 AND id_type=2",engine_lijia)
org_ids=to_list(df)
for p in org_ids:
    org_id=p[0]
    for i in table:
        tab=i[0]
        try:
            engine_lijia.execute("delete from {} WHERE org_id='{}'".format(tab,org_id))
        except BaseException:
            pass
        else:
            pass
