from History.engine import *

def df_fundaccount():
    df_fundaccount = pd.read_sql("SELECT * FROM (SELECT fund_id,fund_name_amac\
     FROM x_fund_info_fundaccount WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010002' and is_used=1)\
    and entry_time>'2017-11-19' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id;",engine_crawl_private)
    df_fundaccount.rename(columns={"fund_id": "fundaccount_id", "fund_name_amac": "fundaccount_name"}, inplace=True)
    return df_fundaccount

def df_private():
    df_private = pd.read_sql("SELECT * FROM (SELECT  fund_id \
    , fund_name_amac \
    FROM x_fund_info_private WHERE fund_id not in  \
    (SELECT source_id FROM base.id_match WHERE source='010003' and is_used=1) \
    and entry_time>'2017-11-20' and version>1 \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_private.rename(columns={"fund_id":"private_id","fund_name_amac":"private_name"},inplace=True)
    df_private["private_id"]=df_private["private_id"].apply(lambda x: 'ID:'+str(x))
    return df_private


def df_securities():
    df_securities = pd.read_sql("SELECT * FROM (SELECT   fund_id \
    , fund_name_amac FROM x_fund_info_securities WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010004' and is_used=1)  \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T-- securities \
    GROUP  BY T.fund_id;",engine_crawl_private)
    df_securities.rename(columns={"fund_id": "securities_id", "fund_name_amac": "securities_name"}, inplace=True)
    return df_securities

def df_futures():
    df_futures= pd.read_sql("SELECT * FROM (SELECT  fund_id, fund_name_amac \
     FROM x_fund_info_futures WHERE fund_id not in \
    (SELECT source_id FROM base.id_match WHERE source='010005' and is_used=1) \
    and entry_time>'2017-11-19'and version>1 \
    ORDER BY version DESC ) AS T -- futures \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_futures.rename(columns={"fund_id": "futures_id", "fund_name_amac": "futures_name"}, inplace=True)
    return df_futures


def df_haomai():
    engine_crawl_private.execute("UPDATE d_fund_info set is_used=0  WHERE fund_id in (select a.fund_id from (select DISTINCT fund_id FROM d_fund_info where is_used=0) as a)")
    df_haomai = pd.read_sql("SELECT * FROM (SELECT fund_id,\
    fund_full_name FROM d_fund_info WHERE fund_id NOT IN (SELECT source_id FROM base.id_match where source='020001')\
    and source_id = '020001' \
    ORDER BY version DESC ) AS T \
    GROUP  BY T.fund_id",engine_crawl_private)
    df_haomai.rename(columns={"fund_id": "haomai_id", "fund_full_name": "haomai_name"}, inplace=True)
    return df_haomai

def df_info():
    info_fund_name=pd.read_sql("select fund_id,fund_full_name from fund_info where fund_name is null",engine_base)
    return info_fund_name




df0=pd.DataFrame([range(10000)]).T
df0.rename(columns={0: "num"}, inplace=True)
df5=df_haomai()
df1=df_private()
df2=df_fundaccount()
df3=df_securities()
df4=df_futures()
df6=df_info()



df=df0.join([df1,df2,df3,df4,df5],how='left')
df["统计数量"]="|"
df["haomai"]=None
df.iloc[0, -1] =len(df5["haomai_id"])
df["private"]=None
df.iloc[0, -1] =len(df1["private_id"])
df["fundaccount"]=None
df.iloc[0, -1] =len(df2["fundaccount_id"])
df["securities"]=None
df.iloc[0, -1] =len(df3["securities_id"])
df["futures"]=None
df.iloc[0, -1] =len(df4["futures_id"])
df["fund_name"]=None
df.iloc[0, -1] =len(df6["fund_full_name"])










def d_person_info():
    df=pd.read_sql("SELECT * FROM `d_person_info` t1 \
    JOIN (SELECT person_id, MAX(version) v FROM d_person_info GROUP BY person_id) t2 \
    ON t1.person_id = t2.person_id AND t1.version = t2.v \
    where t1.person_id not in (SELECT source_id FROM base.id_match where id_type=3 and source in ('020001','020002'))",engine_crawl_private)
    df["person_name"]=df["person_name"].apply(lambda x: x.strip())
    a=len(df)
    return a

def d_org_person():
    df=pd.read_sql("select MAX(VERSION),org_id,org_name,person_id,person_name,source_id,duty,entry_time FROM d_org_person \
    where  person_id not in (SELECT source_id FROM base.id_match WHERE id_type=3 )  GROUP BY person_id;",engine_crawl_private)
    a=len(df)
    return a
def d_person_org():
    df=pd.read_sql("select MAX(VERSION),org_id,org_name,person_id,person_name,source_id,entry_time FROM d_person_org \
    where  person_id not in (SELECT source_id FROM base.id_match WHERE id_type=3 )  GROUP BY person_id;",engine_crawl_private)
    a = len(df)
    return a

df["d_person_info"]=None
df.iloc[0, -1] =d_person_info()

df["d_org_person"]=None
df.iloc[0, -1] =d_org_person()


df["d_person_org"]=None
df.iloc[0, -1] =d_person_org()



to_table(df)

