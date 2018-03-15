from engine import *


# 这里插入使用哪个源
dict={'JR100050':'20002',
'JR100549':'20005',
'JR100925':'20001',
'JR100925':'20002',
'JR101124':'20005',
'JR101402':'20005',
'JR101631':'20002',
'JR101783':'20002',
'JR101791':'20002',
'JR101794':'20002',
'JR101798':'20002',
'JR101927':'20002',
'JR101930':'20002',
'JR101995':'20002',
'JR102053':'20002',
'JR102095':'20002',
'JR102179':'20001',
'JR102702':'20001',
'JR102869':'20002',
'JR103633':'20002',
'JR103863':'20002',
'JR103945':'20002',
'JR104341':'20002',
'JR104341':'20003',
'JR104341':'20005',
'JR104471':'20003',
'JR104552':'20002',
'JR104968':'20002',
'JR104968':'20003',
'JR105024':'20002',
'JR105076':'20002',
'JR105379':'20002',
'JR105471':'20002',
'JR105471':'20003',
'JR105723':'20002',
'JR105723':'20003',
'JR105804':'20002',
'JR105804':'20003',
'JR105904':'20002',
'JR106203':'20001',
'JR106266':'20001',
'JR106491':'20001',
'JR106781':'20002',
'JR107815':'20001',
'JR108499':'20001',
'JR108499':'20003',
'JR109189':'20002',
'JR109949':'20007',
'JR109953':'20007',
'JR112544':'20002',
'JR112544':'20003',
'JR112975':'20001',
'JR113321':'20003',
'JR113444':'20002',
'JR113568':'20001',
'JR113568':'20002',
'JR113803':'20001',
'JR114479':'20002',
'JR114867':'20002',
'JR115520':'20003',
'JR115615':'20005',
'JR115855':'20003',
'JR116000':'20002',
'JR116917':'20005',
'JR117395':'20002',
'JR117515':'20002',
'JR117811':'20001',
'JR117811':'20002',
'JR118148':'20001',
'JR118148':'20003',
'JR118607':'20001',
'JR118688':'20001',
'JR118813':'20001',
'JR119160':'20002',
'JR119651':'20001',
'JR119754':'20001',
'JR119916':'20001',
'JR119919':'20001',
'JR119919':'20002',
'JR119948':'20002',
'JR120104':'20002',
'JR120505':'20002',
'JR120750':'20002',
'JR120789':'20001',
'JR120967':'20001',
'JR121137':'20001',
'JR121211':'20001',
'JR121321':'20001',
'JR121483':'20001',
'JR122762':'20002',
'JR123090':'20001',
'JR123090':'20003',
'JR123199':'20002',
'JR123199':'20003',
'JR123277':'20002',
'JR123277':'20003',
'JR123390':'20002',
'JR123390':'20003',
'JR123485':'20002',
'JR123485':'20003',
'JR123696':'20002',
'JR123980':'20005',
'JR125360':'20001',
'JR125360':'20002',
'JR125367':'20002',
'JR125444':'20002',
'JR125463':'20002',
'JR125723':'20001',
'JR125723':'20002',
'JR126014':'20001',
'JR126091':'20001',
'JR126091':'20002',
'JR126091':'20003',
'JR126226':'20001',
'JR126562':'20001',
'JR126562':'20002',
'JR126562':'20003',
'JR126682':'20001',
'JR126682':'20003',
'JR126682':'20007',
'JR127405':'20001',
'JR128759':'20002',
'JR128780':'20002',
'JR130715':'20002',
'JR130736':'20001',
'JR131121':'20001',
'JR131176':'20001',
'JR131240':'20002',
'JR131443':'20001',
'JR131443':'20002',
'JR131443':'20003',
'JR131724':'20001',
'JR131724':'20002',
'JR131724':'20003',
'JR132259':'20002',
'JR132349':'20001',
'JR132352':'20001',
'JR132355':'20001',
'JR132358':'20001',
'JR132361':'20001',
'JR132364':'20001',
'JR132395':'20001',
'JR132506':'20001',
'JR134037':'20002',
'JR134068':'20002',
'JR134228':'20001',
'JR134228':'40013',
'JR134231':'20001',
'JR134231':'40013',
'JR134250':'20002',
'JR134254':'20002',
'JR134266':'20001',
'JR134266':'40002',
'JR134284':'20001',
'JR134284':'20002',
'JR134284':'4',
'JR134576':'20002',
'JR134646':'20002',
'JR134741':'20001',
'JR134741':'20002',
'JR134741':'20003',
'JR134765':'20002',
'JR135434':'20002',
'JR135853':'20002',
'JR135878':'20002',
'JR136342':'20001',
'JR136350':'20002',
'JR136350':'40025',
'JR136363':'20001',
'JR136476':'20002',
'JR136476':'20003',
'JR137272':'20002',
'JR137304':'20002',
'JR137325':'20002',
'JR137351':'20002',
'JR138076':'40013',
'JR138104':'20002',
'JR138129':'20002',
'JR138129':'40013',
'JR138156':'20002',
'JR138156':'40025',
'JR138159':'40025',
'JR138339':'20002',
'JR138473':'20002',
'JR138484':'20002',
'JR138763':'20002',
'JR138972':'20002',
'JR139098':'20002',
'JR139339':'20002',
'JR139339':'20003',
'JR139670':'20002',
'JR139805':'20002',
'JR139869':'20002',
'JR140145':'20002',
'JR140380':'20002',
'JR140380':'20003',
'JR140524':'20002',
'JR140700':'20001',
'JR140700':'20002',
'JR140700':'20003',
'JR141354':'20002',
'JR141391':'20002',
'JR141532':'40002',
'JR141553':'20001',
'JR141553':'20002',
'JR141553':'40025',
'JR141708':'20002',
'JR142119':'20002',
'JR142772':'20001',
'JR142964':'20002',
'JR143836':'20002',
'JR147281':'40013',
'JR147290':'20001',
'JR147290':'40013',
'JR147593':'20002',
'JR147815':'20002',
'JR148198':'20002',
'JR148333':'20002',
'JR148333':'20003',
'JR148864':'40013',
'JR150386':'20002',
'JR150442':'20002',
'JR150537':'20002',
'JR150811':'20002',
'JR151012':'20001',
'JR151012':'20002',
'JR151224':'20002',
'JR153372':'20002',
'JR153444':'20002',
'JR153749':'20002',
'JR153859':'20002',
'JR153922':'20002',
'JR154012':'20002',
'JR154203':'40013',
'JR155569':'20002',
'JR155649':'20001',
'JR155649':'20002',
'JR155754':'20002',
'JR155826':'20002',
'JR155981':'20001',
'JR156202':'40013',
'JR157138':'20001',
'JR157226':'20001',
'JR157226':'40013',
'JR157229':'20001',
'JR157229':'40013',
'JR157414':'20001',
'JR157712':'20002',
'JR157801':'20002',
'JR157867':'20002',
'JR159569':'20001'}


def is_B0(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=0 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B1(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=1 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B2(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=2 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B4(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=4 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))

def is_B3(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=3 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
def is_B5(JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=5 where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(JR, statistic_date))
# def is_B8(JR,statistic_date):
#     return engine_data_test.execute("update fund_nv_data_standard_test set is_abnormal=8 where fund_id='{}' and statistic_date ='{}' ".format(JR, statistic_date))

def is_B8(JR,statistic_date):
    return engine_data_test.execute("INSERT INTO fund_nv_data_standard_3000 (fund_id,statistic_date, is_abnormal)  VALUES ('{}' ,'{}' ,8)".format(JR, statistic_date))

def is_B9(JR,statistic_date):
    return engine_data_test.execute("INSERT INTO fund_nv_data_standard_3000 (fund_id,statistic_date, is_abnormal)  VALUES ('{}' ,'{}' ,9)".format(JR, statistic_date))

# def is_B9(JR,statistic_date):
#     return engine_data_test.execute("update fund_nv_data_standard_3000 set is_abnormal=9 where fund_id='{}' and statistic_date='{}' ".format(JR, statistic_date))

def to_remark(remark,JR,statistic_date):
    return engine_data_test.execute("update fund_nv_data_standard_3000 set remark='{}' where fund_id='{}' and statistic_date\
                                                    ='{}' ".format(remark,JR, statistic_date))



def yansuan(JR):
    df=pd.read_sql("select fund_id,statistic_date,nav,added_nav from fund_nv_data_standard WHERE fund_id='{}'".format(JR),engine_base)
    if df.empty:
        pass
    else:
        df2=pd.read_sql("select fund_id,statistic_date,nav,added_nav from fund_nv_data_source_copy2 where fund_id='{}' and source_id='{}'".format(JR,dict.get(JR)),engine_base)

        nv = pd.merge(df, df2, how='outer', on=['fund_id', 'statistic_date', 'nav','added_nav'])
        to_sql("fund_nv_data_standard_3000", engine_data_test, nv, type="update")
        if nv.empty:
            pass
        elif (nv["nav"] == nv["added_nav"]).all():
            try:
                diff = abs(nv[["statistic_date", "nav", "added_nav"]] - nv[["statistic_date", "nav", "added_nav"]].shift(1))
                diff = diff[1:]
                diff["statistic_date"] = diff["statistic_date"].apply(lambda x: x.days)
                diff.columns = ["days", "nav", "added_nav"]
                time_1 = nv["statistic_date"]
                diff["nca"] = (nv["nav"] - nv["nav"].shift(1)) / nv["nav"].shift(1)
                diff["nca2"] = (nv["nav"] - nv["nav"].shift(2)) / nv["nav"].shift(2)
                # diff["dca"] = nv["statistic_date"] - nv["statistic_date"].shift(1)
                difff = diff.join(time_1)  # 总表
                dif = difff.loc[(difff["nca"].abs() > 0.1) & (difff["nca2"].abs() > 0.12)]
                # dif["fund_id1"] = JR
                cc = pd.DataFrame(dif)
                cc["fund_id"] = JR
                cc["is_abnormal"] = 1
                df_3 = cc[["fund_id", "is_abnormal", "statistic_date"]]
                to_sql("fund_nv_data_standard_3000", engine_data_test, df,
                       type="update")  # ----------------------------------------输入1的异常
                if df_3.empty:
                    print(JR)
                    print("No problem")
                else:
                    print(JR)
                    print("异常")


                bb = np.array(dif)  # np.ndarray()
                aa = bb.tolist()  # list
                num = len(aa)
                for i in range(num):
                    day = aa[i][0]
                    bo = abs(aa[i][4])
                    stat = str(aa[i][5])

                    if day <= 3 and bo < 0.15:
                        is_B2(JR, stat)
                        print("小小波")
                        pass
                    elif 5 >= day > 3 and bo < 0.2:
                        is_B3(JR, stat)
                        print("小波")
                        pass

                    elif 14 >= day >= 6 and bo < 0.25:
                        is_B4(JR, stat)
                        print("中波动")

                    elif day >= 21 and bo < 0.5:
                        is_B5(JR, stat)
                        print("大波动月更")

                    else:
                        is_B1(JR, stat)
            except BaseException:
                pass
            else:
                pass


        else:
            #
            c2 = nv.loc[nv["nav"] != nv["added_nav"]]

            try:
                c2["nnn"] = round((c2["added_nav"] - c2["nav"]), 3)
                c2["ccc"] = round((c2["added_nav"] / c2["nav"]), 3)

                cnt = c2["nnn"].value_counts()
                ccc = c2["ccc"].value_counts()
                # cnt = c2["nnn"].value_counts()
                # cnt[cnt < cnt.mean()]
                y = cnt[cnt <= 1].index
                z = ccc[ccc <= 1].index
                Y = len(y)
                Z = len(z)
                # if y.empty:
                if Z > Y:
                    if y.empty:
                        pass
                    else:
                        for i in y:
                            a = c2[c2.nnn == i]
                            bb = np.array(a)  # np.ndarray()
                            aa = bb.tolist()  # list
                            JR = aa[0][0]
                            stat = str(aa[0][2])
                            is_B8(JR, stat)
                            print(JR)
                            print("分红错")
                elif Z < Y:
                    if z.empty:
                        pass
                    else:
                        for i in z:
                            a = c2[c2.ccc == i]
                            bb = np.array(a)  # np.ndarray()
                            aa = bb.tolist()  # list
                            JR = aa[0][0]
                            stat = str(aa[0][2])
                            is_B9(JR, stat)
                            print(JR)
                            print("拆分错")
                else:
                    pass

            except BaseException:
                pass
            else:
                pass

def check():
    df=pd.read_sql("SELECT t.fund_id,t.statistic_date,t.is_abnormal,b.nav FROM fund_nv_data_standard_3000 as t LEFT JOIN base.fund_nv_data_standard as b ON t.fund_id=b.fund_id and \
     t.statistic_date=b.statistic_date AND t.is_abnormal in (1,2,3,4,5) WHERE b.nav is not NULL",engine_data_test)
    # df=pd.read_sql("select fund_id,statistic_date,is_abnormal,")

    # df["statistic_date"]=df["statistic_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    df["nav"] = df["nav"].apply(lambda x: '%.4f' % x)
    x=to_list(df)
    for i in x:
        JR=i[0]
        time=i[1]
        nav=i[3]
        try:
            df=pd.read_sql("SELECT fund_id,data_source,data_source_name,nav,added_nav\
                            from fund_nv_data_source where fund_id='{}' and statistic_date='{}'".format(JR,time),engine_base)
            df["nav"] = df["nav"].apply(lambda x: x / 100 if x > 60 else x)
            df["nav"] = df["nav"].apply(lambda x: '%.4f' % x)

            num=len(df)
            if num>=2:
                c = df[df.nav!=nav]
                nc=len(c)
                if c.empty:
                    remark="全对"
                    print(remark)
                    to_remark(remark,JR,time)

                else:
                    y=str(round((1-nc/num)*100,1))
                    print(y)
                    to_remark(y,JR, time)

            else:
                pass
        except BaseException:
            print("错误")
            pass
        else:
            pass




# def len_JR():
#     JR_all = pd.read_sql("select DISTINCT fund_id FROM fund_nv_data_standard", engine_base)
#     # JR_all = pd.read_sql("SELECT DISTINCT fund_id from fund_nv_data_standard where nav<0.1", engine_base)
#     bb = np.array(JR_all)  # np.ndarray()
#     vv = bb.tolist()  # list
#     ALL=[]
#     for i in vv:
#         for o in i:
#             ALL.append(o)
#
#     a=len(ALL)
#     b=int(a/10)
#     all = []
#     for i in range(b):
#         a=ALL[i*10:(i*10+10)]
#         all.append(a)
#     all.append(ALL[b*10:])
#     return all
#



# all=len_JR()
# pool = ThreadPool(20)
# pool.map(crawl,all)
# pool.close()
# pool.join()
jr_list=['JR100050',
'JR100549',
'JR100925',
'JR101124',
'JR101402',
'JR101631',
'JR101783',
'JR101791',
'JR101794',
'JR101798',
'JR101927',
'JR101930',
'JR101995',
'JR102053',
'JR102095',
'JR102179',
'JR102702',
'JR102869',
'JR103633',
'JR103863',
'JR103945',
'JR104341',
'JR104471',
'JR104552',
'JR104968',
'JR105024',
'JR105076',
'JR105379',
'JR105471',
'JR105723',
'JR105804',
'JR105904',
'JR106203',
'JR106266',
'JR106491',
'JR106781',
'JR107815',
'JR108499',
'JR109189',
'JR109949',
'JR109953',
'JR112544',
'JR112975',
'JR113321',
'JR113444',
'JR113568',
'JR113803',
'JR114479',
'JR114867',
'JR115520',
'JR115615',
'JR115855',
'JR116000',
'JR116917',
'JR117395',
'JR117515',
'JR117811',
'JR118148',
'JR118607',
'JR118688',
'JR118813',
'JR119160',
'JR119651',
'JR119754',
'JR119916',
'JR119919',
'JR119948',
'JR120104',
'JR120505',
'JR120750',
'JR120789',
'JR120967',
'JR121137',
'JR121211',
'JR121321',
'JR121483',
'JR122762',
'JR123090',
'JR123199',
'JR123277',
'JR123390',
'JR123485',
'JR123696',
'JR123980',
'JR125360',
'JR125367',
'JR125444',
'JR125463',
'JR125723',
'JR126014',
'JR126091',
'JR126226',
'JR126562',
'JR126682',
'JR127405',
'JR128759',
'JR128780',
'JR130715',
'JR130736',
'JR131121',
'JR131176',
'JR131240',
'JR131443',
'JR131724',
'JR132259',
'JR132349',
'JR132352',
'JR132355',
'JR132358',
'JR132361',
'JR132364',
'JR132395',
'JR132506',
'JR134037',
'JR134068',
'JR134228',
'JR134231',
'JR134250',
'JR134254',
'JR134266',
'JR134284',
'JR134576',
'JR134646',
'JR134741',
'JR134765',
'JR135434',
'JR135853',
'JR135878',
'JR136342',
'JR136350',
'JR136363',
'JR136476',
'JR137272',
'JR137304',
'JR137325',
'JR137351',
'JR138076',
'JR138104',
'JR138129',
'JR138156',
'JR138159',
'JR138339',
'JR138473',
'JR138484',
'JR138763',
'JR138972',
'JR139098',
'JR139339',
'JR139670',
'JR139805',
'JR139869',
'JR140145',
'JR140380',
'JR140524',
'JR140700',
'JR141354',
'JR141391',
'JR141532',
'JR141553',
'JR141708',
'JR142119',
'JR142772',
'JR142964',
'JR143836',
'JR147281',
'JR147290',
'JR147593',
'JR147815',
'JR148198',
'JR148333',
'JR148864',
'JR150386',
'JR150442',
'JR150537',
'JR150811',
'JR151012',
'JR151224',
'JR153372',
'JR153444',
'JR153749',
'JR153859',
'JR153922',
'JR154012',
'JR154203',
'JR155569',
'JR155649',
'JR155754',
'JR155826',
'JR155981',
'JR156202',
'JR157138',
'JR157226',
'JR157229',
'JR157414',
'JR157712',
'JR157801',
'JR157867',
'JR159569']
for  JR in jr_list:
    yansuan(JR)
check()
print("DOWN"+'\n'+now2)

