import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import re


engine = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171,'base', ), connect_args={"charset": "utf8"}, echo=True, )


df=pd.read_sql('select user_id,resume from manager_info where sex is NULL ',engine)
print(df)


train_data = np.array(df)#np.ndarray()
list=train_data.tolist()#list
print(list)


x=[['P000001','先生是投顾'],['P000002','投资经理']]
for a in x:

    patt_gender = "先生|男"
    sex=re.search(patt_gender, a).group()
    dict={
        "先生":"男"
    }
    y=dict[sex]
    print(y)