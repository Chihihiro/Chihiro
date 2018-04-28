from engine import *

import re


def sub_wrong_to_none(x):
    if type(x) is str:
        s = re.sub("\s|--|---|-", "", x)
        if s == "":
            return None

    return x


def check_dataframe(dataframe, columns_to_check):
    dataframe[columns_to_check] = dataframe[columns_to_check].applymap(lambda x: sub_wrong_to_none(x))
    return dataframe

def diff(listA,listB):
    retA = [i for i in listA if i in listB]
    return retA

import pandas as pd

T1='fund_org_mapping'
T2='fund_org_mapping_old_20180119'

str3='fund_id'
str4='fund_id'
info=pd.read_sql("select {} from {}".format(str3,T1),engine_base)
info2=pd.read_sql("select {} from {}".format(str4,T2),engine_base_test)

T3 = 'fund_type_source'
T4 = 'fund_type_source_old_20180308'

info3=pd.read_sql("select {} from {}".format(str3,T3),engine_base)
info4=pd.read_sql("select {} from {}".format(str4,T4),engine_base_test)

T5 ='fund_manager_mapping'
T6 = 'fund_manager_mapping_20180322'

info5=pd.read_sql("select {} from {}".format(str3,T5),engine_base)
info6=pd.read_sql("select {} from {}".format(str4,T6),engine_base_test)

T7 = 'fund_asset_scale'
T8 = 'fund_asset_scale_old_20180123'
info7=pd.read_sql("select {} from {}".format(str3,T7),engine_base)
info8=pd.read_sql("select {} from {}".format(str4,T8),engine_base_test)


str2 = 'user_id'
str1 = 'person_id'
T9 = 'person_info'
T10 = 'manager_info_20180202'
info9=pd.read_sql("select {} from {}".format(str1,T9),engine_base)
info10=pd.read_sql("select {} from {} GROUP BY user_id".format(str2,T10),engine_base_test)

# xs = [T3,T1,T9]
# ys=[len(info3),len(info),len(info9)]
# ys2=[len(info4),len(info2),len(info10)]

TM = "org_person_info"
xs = [T1,T3,T5,T9,TM]

# T3 = "org_person_mapping"
# a1 = int(len(info)/len(info2)*100)
# a2 = int(len(info3)/len(info4)*100)
# a3 = int(len(info5)/len(info6)*100)
# a4 = int(len(info7)/len(info8)*100)
# a5 = int(71915/37364*100)

a1 = int(len(info)/10)
b1 = int(len(info2)/10)
a2 = int(len(info3)/10)
b2 = int(len(info4)/10)
a3 = len(info5)
b3 = len(info6)
a4 = len(info9)
b4 = len(info10)
a5 = 71915
b5 = 37364

ys = [a1,a2,a3,a4,a5]
ys2 = [b1,b2,b3,b4,b5]


T11='person_info'
T12='manager_info_20180202'
str5 = 'duty,education,background'

info11=pd.read_sql("SELECT a.person_id,a.education,a.background,b.duty FROM `person_info` as a \
LEFT JOIN (SELECT person_id,duty from org_person_mapping) as b on a.person_id=b.person_id",engine_base)
info12=pd.read_sql("select {} from {} GROUP BY user_id".format(str5,T10),engine_base_test)

a = len(info11)
b = info11.iloc[:,3].dropna()
c = info11.iloc[:,1].dropna()
d = info11.iloc[:,2].dropna()

ttt = ["duty","education","background"]
info12 =check_dataframe(info12, ttt)
a2 = len(info12)
# info12.iloc[:,2] = info12.iloc[:,2].apply(lambda x: sub_wrong_to_none(x))
b2 = info12.iloc[:,0].dropna()
c2 = info12.iloc[:,1].dropna()
d2 = info12.iloc[:,2].dropna()



j1 = pd.read_sql("select resume FROM person_description",engine_base)
j11 = check_dataframe(j1)
aaa =j11[j11["resume"].notnull()]
j2 = pd.read_sql("select resume FROM manager_info",engine_base)
j22 = check_dataframe(j2)
bbb =j22[j22["resume"].notnull()]


xs = ['num','duty','education','background',"resume"]
ys = [a,len(b),len(c),len(d),len(aaa)]
ys2 =[a2,len(b2),len(c2),len(d2),len(bbb)]


ys = [76468, 71915, 39764, 13373, 9285, 7833]