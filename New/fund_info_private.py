from engine import *


def df_fundaccount():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name_amac,reg_code_amac \
     FROM x_fund_info_fundaccount WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010002' and is_used=1)\
    and entry_time>'2017-11-19' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id;",engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_fundaccount

def df_private():
    df_private = pd.read_sql("SELECT * FROM (SELECT  fund_id \
    , fund_name_amac,reg_code_amac \
    FROM x_fund_info_private WHERE fund_id not in  \
    (SELECT source_id FROM base.id_match WHERE source='010003' and is_used=1) \
    and entry_time>'2017-11-20' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_private.rename(columns={"fund_id":"source_id","fund_name_amac":"test_name"},inplace=True)
    # df_private["private_id"]=df_private["private_id"].apply(lambda x: 'ID:'+str(x))
    return df_private


def df_securities():
    df_securities = pd.read_sql("SELECT * FROM (SELECT   fund_id \
    , fund_name_amac,reg_code_amac FROM x_fund_info_securities WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010004' and is_used=1)  \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T-- securities \
    GROUP  BY T.fund_id;",engine_crawl_private)
    df_securities.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_securities

def df_futures():
    df_futures= pd.read_sql("SELECT * FROM (SELECT  fund_id, fund_name_amac,reg_code_amac \
     FROM x_fund_info_futures WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010005' and is_used=1) \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T -- futures \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_futures.rename(columns={"fund_id": "source_id", "fund_name_amac": "test_name"}, inplace=True)
    return df_futures


def df_haomai():
    engine_crawl_private.execute("UPDATE d_fund_info set is_used=0  WHERE fund_id in (select a.fund_id from (select DISTINCT fund_id FROM d_fund_info where is_used=0) as a)")
    df_haomai = pd.read_sql("SELECT * FROM (SELECT fund_id,\
    fund_full_name FROM d_fund_info WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020001')\
    and source_id = '020001' \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_haomai.rename(columns={"fund_id": "haomai_id", "fund_full_name": "test_name"}, inplace=True)
    return df_haomai

def df_info():
    info_fund_name=pd.read_sql("select fund_id,fund_full_name from fund_info where fund_name is null",engine_base)
    return info_fund_name
dict_table = {"010002":"x_fund_info_fundaccount",
              "010003":"x_fund_info_private",
              "010004":"x_fund_info_securities",
              "010005":"x_fund_info_futures",
              "020001":"d_fund_info",
              "020002":"d_fund_info",
              "020003":"d_fund_info",
              }
def fund_full_name(fund_id):
    df=pd.read_sql("select source_id,source from id_match WHERE matched_id='{}' \
    and is_used=1 AND source not in ('010001','000001','020004','020005','020007') and source not like '03%%' and source not like'04%%' and source not like'05%%'".format(fund_id),engine_base)
    num=len(df)

    df['fund_tabel']=df['source'].apply(lambda x: dict_table.get(x))
    df["fund_full_name"]=None
    for i in range(num):
        a=df.iloc[i,0]
        b=df.iloc[i,1]
        c=df.iloc[i,2]
        if b in ('020001','020002','020003'):
            name=pd.read_sql("SELECT DISTINCT fund_full_name FROM d_fund_info WHERE fund_id='{}' and source_id='{}'".format(a,b),engine_crawl_private)
            try:
                full_name=name.iloc[0,0]
            except BaseException:
                pass
            else:
                full_name='空'
            df.iloc[i,3]=full_name

        else:
            name=pd.read_sql("SELECT DISTINCT fund_name_amac FROM {} where fund_id='{}'".format(c,a),engine_crawl_private)
            full_name=name.iloc[0,0]
            df.iloc[i,3]=full_name
    print(df)
    L=df["fund_full_name"]
    list=to_list(L)
    return list


def generate_id(start_from, length):
    ids = [(start_from + i) for i in range(0, length)]
    return ["JR" + (6 - len(str(x))) * "0" + str(x) for x in ids]




table_reg_code=pd.read_sql("select reg_code,fund_id from fund_info where reg_code is not NULL",engine_base)
table_reg_code["reg_code"]=table_reg_code["reg_code"].apply(lambda x: x.strip())
dict ={key:value for key,value in zip(table_reg_code["reg_code"],table_reg_code["fund_id"])}
yes='yes'
no='no'
table1=pd.read_sql("select fund_full_name,fund_id from fund_info",engine_base)
dict1 ={key:value for key,value in zip(table1["fund_full_name"],table1["fund_id"])}

def test2(private_null):
    if private_null.empty:
        print("米有匹配到")
        c = []
        d = []
        return c, d
    else:
        print("have")
        private_null["fund_id"] = private_null["test_name"].apply(lambda x: dict1.get(x))
        no = private_null.fillna("空")
        c = no[no['fund_id'] != '空']
        d = no[no['fund_id'] == '空']
        return c,d

def test1(private):
    if private.empty:
        print("米有匹配到")
        a = []
        b = []
        return a, b

    else:
        print("have")
        private["same"] = list(
        map(lambda x, y: yes if y in fund_full_name(x) else no, private["fund_id"], private["test_name"]))
        a = private[private["same"] == 'yes']
        b = private[private["same"] == 'no']
        return a, b

def re_len(a,b):
    if len(a)==0 and len(b)>0:
        result=b
        return result
    elif len(b)==0 and len(a)>0:
        result = a
        return result
    elif len(a)==0 and len(b)==0:
        result=[]
        return result
    else:
        result=a.append(b)
        return result

def id_match(fund_info,source):
    fund_info["fund_id"]=fund_info["reg_code_amac"].apply(lambda x: dict.get(x) )
    private=fund_info.loc[fund_info["fund_id"].notnull()]
    private_null=fund_info.loc[fund_info["fund_id"].isnull()]
    t1=test1(private)
    t2=test2(private_null)
    a=t1[0]
    b=t1[1]
    c=t2[0]
    d=t2[1]

    # result = pd.merge(a, c, how='outer')
    # result2 = pd.merge(b, d, how='outer')

    result =re_len(a,c)
    result2 = re_len(b,d)

    if len(result) == 0:
        new_fund_id = result2["source_id"]
        new_fund=pd.DataFrame(new_fund_id)
        new_fund["source"]=source
        new_fund["id_type"]=1
        new_fund["is_used"]=1
        new_fund["is_del"]=0
        jr=pd.read_sql("select max(matched_id) from id_match where  id_type=1",engine_base)
        maxjr=jr.iloc[0,0]
        m=int(maxjr.replace("JR",""))
        new_fund["matched_id"]= generate_id(m+1, len(new_fund))
        return new_fund

    elif len(result2) == 0:
        new1=result.loc[:,['fund_id','source_id']]
        new1["source"]=source
        new1["id_type"]=1
        new1["is_used"]=1
        new1["is_del"]=0
        new1.rename(columns={"fund_id": "matched_id"}, inplace=True)
        return new1
    else:
        new1=result.loc[:,['fund_id','source_id']]
        new1["source"]=source
        new1["id_type"]=1
        new1["is_used"]=1
        new1["is_del"]=0
        new_fund_id = result2["source_id"]
        new_fund=pd.DataFrame(new_fund_id)
        new_fund["source"]=source
        new_fund["id_type"]=1
        new_fund["is_used"]=1
        new_fund["is_del"]=0
        jr=pd.read_sql("select max(matched_id) from id_match where  id_type=1",engine_base)
        maxjr=jr.iloc[0,0]
        m=int(maxjr.replace("JR",""))
        new_fund["fund_id"]= generate_id(m+1, len(new_fund))
        re = new1.append(new_fund)
        re.rename(columns={"fund_id":"matched_id"},inplace=True)
        return re



source_fundaccount='010002'
source_private='010003'
source_securities='010004'
source_futures='010005'


fund_fundaccount=df_fundaccount()
over1=id_match(fund_fundaccount,source_fundaccount)

to_sql("id_match", engine_base, over1, type="ignore") #ignore


fund_securities=df_securities()
over3=id_match(fund_securities,source_securities)
to_sql("id_match", engine_base, over3, type="ignore") #ignore

fund_futures=df_futures()
over4=id_match(fund_futures,source_futures)
to_sql("id_match", engine_base, over4, type="ignore") #ignore



fund_private=df_private()
over2=id_match(fund_private,source_private)
to_sql("id_match", engine_base, over2, type="ignore") #ignore















































def f(x):
    a=x[0]
    b=x[1]
    yes = 'yes'
    no = 'no'
    name_list=fund_full_name(a)
    if b in name_list:
        return yes
    else:
        return no




# private["same"]=private[["fund_id","private_name"]].apply(f, axis=1)


def f(x):
    id=x[0]
    name=x[1]
    name2=fund_full_name(id)
    if name in name2:
        return True
    else:
        return False