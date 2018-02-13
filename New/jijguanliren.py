import pandas as pd
from History.iosjk import to_sql
from sqlalchemy import create_engine
from History.engine import engine_base
import numpy as np

df=pd.read_sql("select fund_id,fund_name,fund_full_name,fund_manager_nominal_ from \
                fund_info where fund_id not in (select fund_id from fund_org_mapping where org_type_code=2) AND fund_manager_nominal_ is not NULL ",engine_base("base"))
AA=df.iloc[:,[0,3]]
train_data = np.array(AA)#np.ndarray()
CC =train_data.tolist()#list
A=[]
for i in CC:
    x=""
    y="---"
    z="-"
    if i[1]==x:
        pass
    elif i[1]==y:
        pass
    elif i[1]==z:
        pass
    else:
        A.append(i)
AAA=pd.DataFrame(A)
AAA.rename(columns={0:'fund_id',1:'fund_manager_nominal'},inplace=True)
AAA["fund_manager_nominal"]=AAA["fund_manager_nominal"].apply(lambda x: x.lstrip())

import numpy as np
train_data = np.array(AAA)#np.ndarray()
BB =train_data.tolist()#list

for i in BB:
    if i[1] =="":
        print(i)
        BB.remove(i)
    else:
        pass




engine_base = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )
# BB=[['JR000033', '兴证证券资产管理有限公司'], ['JR000042', '北方A啊斌']]
# table2=pd.read_sql("select org_id,org_full_name from org_info where org_full_name is not NULL ",engine_base)
# table2["org_full_name"]=table2["org_full_name"].apply(lambda x: x.lstrip())
# dict2 ={key:value for key,value in zip(table2["org_id"],table2["org_full_name"])}


engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ), connect_args={"charset": "utf8"}, echo=True, )

while len(BB)>0:
    gun_all=BB.pop()
    nominal=gun_all[1]
    print(nominal)
    aa = len(nominal)
    if aa==4:
        try:
            org = pd.read_sql("SELECT org_id from org_info WHERE org_name like '{}' AND org_id NOT LIKE 'JG%'".format(nominal),
                              engine_base).iloc[0, 0]
        except BaseException:
            print(nominal + "米有")
        else:
            print(org)
            gun_all.append(org)
            gun_all.append("基金管理人")
            gun_all.append("2")
            df=pd.DataFrame(gun_all)
            DF=df.T
            DF.columns =["fund_id","org_name","org_id","org_type","org_type_code"]
            print(DF)
            engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                                    connect_args={"charset": "utf8"}, echo=True, )
            to_sql("fund_org_mapping", engine5, DF, type="update")
    else:
        try:
            org = pd.read_sql("SELECT org_id,org_name from org_info WHERE org_full_name like '{}'".format(nominal),
                              engine5)
            org_id=org.iloc[0,0]
            org_name=org.iloc[0,1]
        except BaseException:
            print(nominal + "----------米有")
        else:
            gun_all.pop()
            gun_all.append(org_name)
            gun_all.append(org_id)
            gun_all.append("基金管理人")
            gun_all.append("2")
            df = pd.DataFrame(gun_all)
            DF = df.T
            DF.columns = ["fund_id", "org_name", "org_id", "org_type", "org_type_code"]
            print(DF)
            # time.sleep(0.5)
            engine5 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '', 'localhost', 3306, 'test', ),
                                    connect_args={"charset": "utf8"}, echo=True, )
            to_sql("fund_org_mapping", engine5, DF, type="update")




print("DOWN")

# engine_base = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )
#
# table2=pd.read_sql("select org_id,org_full_name from org_info where org_full_name is not NULL ",engine_base)
# table2["org_full_name"]=table2["org_full_name"].apply(lambda x: x.lstrip())
# dict2 ={key:value for key,value in zip(table2["org_id"],table2["org_full_name"])}




