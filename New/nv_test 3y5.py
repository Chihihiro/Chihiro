from engine import *


# 这里插入使用哪个源
dict={'JR133164':'020003',
    'JR003639':'020003',
    'JR102014':'020003',
    'JR042390':'020003',
    'JR003262':'020003',
    'JR077208':'020003',
    'JR003313':'020003',
    'JR003460':'020003',
    'JR003412':'020003',
    'JR003186':'020003',
    'JR003300':'020003',
    'JR003326':'020003',
    'JR003181':'020003',
    'JR003004':'020003',
    'JR128440':'020003',
    'JR083319':'020003',
    'JR049056':'020003',
    'JR003011':'020003',
    'JR100794':'020003',
    'JR003390':'020003',
    'JR128448':'020003',
    'JR002992':'020003',
    'JR035519':'020003',
    'JR111274':'020003',
    'JR032321':'020003',
    'JR003324':'020003',
    'JR090171':'020003',
    'JR003734':'020003',
    'JR003735':'020003',
    'JR003736':'020003',
    'JR003221':'020003',
    'JR003006':'020003',
    'JR128421':'020003',
    'JR003108':'020003',
    'JR003050':'020003',
    'JR003051':'020003',
    'JR003052':'020003',
    'JR003071':'020003',
    'JR003524':'020003',
    'JR128469':'020003',
    'JR101506':'020003',
    'JR112787':'020003',
    'JR112788':'020003',
    'JR090204':'020003',
    'JR128432':'020003',
    'JR128460':'020003',
    'JR101026':'020003',
    'JR133147':'020003',
    'JR107978':'020003',
    'JR107977':'020003',
    'JR127476':'020003',
    'JR128446':'020003',
    'JR128577':'020003',
    'JR099857':'020003',
    'JR128398':'020003',
    'JR128546':'020003',
    'JR041422':'020003',
    'JR059835':'020003',
    'JR106854':'020003',
    'JR133134':'020003',
    'JR061681':'020003',
    'JR028560':'020003',
    'JR128407':'020003',
    'JR059444':'020003',
    'JR058771':'020003',
    'JR035361':'020003',
    'JR034572':'020003',
    'JR042245':'020003',
    'JR055460':'020003',
    'JR080967':'020003',
    'JR027169':'020003',
    'JR057775':'020003',
    'JR004160':'020003',
    'JR127499':'020003',
    'JR045240':'020003',
    'JR054058':'020003',
    'JR004138':'020003',
    'JR069044':'020003',
    'JR004154':'020003',
    'JR003698':'020003',
    'JR066783':'020003',
    'JR128420':'020003',
    'JR128436':'020003',
    'JR003587':'020003',
    'JR107458':'020003',
    'JR128441':'020003',
    'JR128422':'020003',
    'JR003718':'020003',
    'JR003793':'020003',
    'JR003794':'020003',
    'JR003795':'020003',
    'JR003796':'020003',
    'JR003797':'020003',
    'JR003798':'020003',
    'JR003799':'020003',
    'JR003800':'020003',
    'JR003801':'020003',
    'JR003802':'020003',
    'JR003778':'020003',
    'JR128439':'020003',
    'JR101329':'020003',
    'JR106418':'020003',
    'JR106417':'020003',
    'JR174669':'020003',
    'JR003859':'020003',
    'JR128438':'020003',
    'JR128529':'020003',
    'JR029833':'020003',
    'JR106416':'020003',
    'JR101426':'020003',
    'JR003925':'020003',
    'JR003948':'020003',
    'JR106191':'020003',
    'JR106189':'020003',
    'JR106192':'020003',
    'JR071720':'020003',
    'JR110571':'020003',
    'JR133169':'020003',
    'JR052812':'020003',
    'JR101041':'020003',
    'JR107488':'020003',
    'JR106419':'020003',
    'JR003959':'020003',
    'JR004136':'020003',
    'JR000008':'020003',
    'JR128569':'020003',
    'JR004255':'020003',
    'JR004250':'020003',
    'JR087576':'020003',
    'JR128541':'020003',
    'JR099811':'020003',
    'JR174702':'020003',
    'JR107511':'020003',
    'JR090203':'020003',
    'JR130381':'020003',
    'JR112249':'020003',
    'JR101100':'020003',
    'JR004377':'020003',
    'JR004323':'020003',
    'JR133128':'020003',
    'JR082589':'020003',
    'JR115108':'020003',
    'JR099825':'020003',
    'JR046089':'020003',
    'JR107238':'020003',
    'JR114957':'020003',
    'JR112233':'020003',
    'JR122363':'020003',
    'JR109910':'020003',
    'JR112235':'020003',
    'JR004523':'020003',
    'JR012190':'020003',
    'JR110077':'020003',
    'JR012256':'020003',
    'JR004646':'020003',
    'JR084034':'020003',
    'JR130379':'020003',
    'JR100688':'020003',
    'JR110179':'020003',
    'JR004589':'020003',
    'JR174828':'020003',
    'JR108311':'020003',
    'JR128090':'020003',
    'JR036425':'020003',
    'JR127968':'020003',
    'JR004780':'020003',
    'JR077413':'020003',
    'JR012370':'020003',
    'JR004722':'020003',
    'JR067785':'020003',
    'JR012055':'020003',
    'JR098721':'020003',
    'JR107019':'020003',
    'JR098829':'020003',
    'JR098710':'020003',
    'JR012516':'020003',
    'JR078422':'020003',
    'JR051120':'020003',
    'JR109400':'020003',
    'JR174763':'020003',
    'JR073084':'020003',
    'JR100398':'020003',
    'JR004622':'020003',
    'JR074063':'020003',
    'JR004750':'020003',
    'JR084173':'020003',
    'JR012431':'020003',
    'JR073754':'020003',
    'JR078248':'020003',
    'JR012443':'020003',
    'JR115526':'020003',
    'JR055510':'020003',
    'JR005763':'020003',
    'JR066038':'020003',
    'JR068431':'020003',
    'JR174830':'020003',
    'JR053706':'020003',
    'JR004435':'020003',
    'JR098824':'020003',
    'JR107505':'020003',
    'JR101740':'020003',
    'JR100420':'020003',
    'JR110684':'020003',
    'JR000423':'020003',
    'JR006389':'020003',
    'JR005002':'020003',
    'JR083221':'020003',
    'JR075917':'020003',
    'JR031734':'020003',
    'JR084050':'020003',
    'JR107820':'020003',
    'JR100717':'020003',
    'JR074077':'020003',
    'JR078359':'020003',
    'JR005148':'020003',
    'JR013177':'020003',
    'JR031742':'020003',
    'JR058294':'020003',
    'JR107194':'020003',
    'JR054246':'020003',
    'JR066642':'020003',
    'JR122531':'020003',
    'JR004645':'020003',
    'JR083212':'020003',
    'JR012845':'020003',
    'JR004860':'020003',
    'JR048541':'020003',
    'JR111788':'020003',
    'JR012311':'020003',
    'JR083220':'020003',
    'JR072382':'020003',
    'JR110686':'020003',
    'JR005105':'020003',
    'JR109782':'020003',
    'JR078166':'020003',
    'JR005251':'020003',
    'JR013255':'020003',
    'JR107504':'020003',
    'JR094857':'020003',
    'JR094856':'020003',
    'JR094855':'020003',
    'JR107503':'020003',
    'JR109784':'020003',
    'JR038404':'020003',
    'JR036295':'020003',
    'JR115796':'020003',
    'JR083587':'020003',
    'JR111549':'020003',
    'JR013814':'020003',
    'JR013900':'020003',
    'JR114857':'020003',
    'JR014995':'020003',
    'JR014922':'020003',
    'JR064032':'020003',
    'JR098660':'020003',
    'JR053280':'020003',
    'JR071533':'020003',
    'JR115795':'020003',
    'JR013632':'020003',
    'JR014143':'020003',
    'JR002575':'020003',
    'JR013895':'020003',
    'JR013896':'020003',
    'JR115600':'020003',
    'JR006781':'020003',
    'JR005375':'020003',
    'JR030096':'020003',
    'JR008933':'020003',
    'JR008936':'020003',
    'JR008945':'020003',
    'JR008944':'020003',
    'JR008942':'020003',
    'JR008947':'020003',
    'JR008935':'020003',
    'JR008938':'020003',
    'JR008948':'020003',
    'JR008943':'020003',
    'JR008950':'020003',
    'JR008941':'020003',
    'JR008949':'020003',
    'JR008937':'020003',
    'JR008940':'020003',
    'JR008934':'020003',
    'JR008946':'020003',
    'JR008952':'020003',
    'JR008939':'020003',
    'JR008930':'020003',
    'JR008951':'020003',
    'JR008931':'020003',
    'JR008932':'020003',
    'JR005410':'020003',
    'JR005409':'020003',
    'JR005439':'020003',
    'JR005435':'020003',
    'JR014819':'020003',
    'JR069919':'020003',
    'JR078485':'020003',
    'JR005905':'020003',
    'JR027383':'020003',
    'JR005532':'020003',
    'JR112328':'020003',
    'JR006429':'020003',
    'JR008162':'020003',
    'JR005554':'020003',
    'JR005926':'020003',
    'JR056605':'020003',
    'JR112252':'020003',
    'JR000790':'020003',
    'JR100084':'020003',
    'JR100085':'020003',
    'JR027548':'020003',
    'JR006655':'020003',
    'JR029009':'020003',
    'JR007008':'020003',
    'JR006178':'020003',
    'JR069986':'020003',
    'JR073844':'020003',
    'JR077178':'020003',
    'JR051031':'020003',
    'JR005583':'020003',
    'JR017791':'020003',
    'JR006712':'020003',
    'JR006793':'020003',
    'JR060684':'020003',
    'JR059757':'020003',
    'JR032932':'020003',
    'JR032053':'020003',
    'JR006017':'020003',
    'JR080898':'020003',
    'JR069925':'020003',
    'JR005920':'020003',
    'JR005918':'020003',
    'JR078733':'020003',
    'JR063370':'020003',
    'JR066084':'020003',
    'JR006040':'020003',
    'JR006794':'020003',
    'JR009255':'020003',
    'JR029170':'020003',
    'JR115511':'020003',
    'JR006257':'020003',
    'JR015111':'020003',
    'JR076176':'020003',
    'JR006162':'020003',
    'JR027447':'020003',
    'JR027448':'020003',
    'JR027449':'020003',
    'JR080813':'020003',
    'JR027493':'020003',
    'JR006215':'020003',
    'JR006216':'020003',
    'JR006217':'020003',
    'JR006218':'020003',
    'JR006219':'020003',
    'JR006220':'020003',
    'JR080950':'020003',
    'JR080955':'020003',
    'JR007339':'020003',
    'JR090010':'020003',
    'JR006946':'020003',
    'JR006947':'020003',
    'JR006948':'020003',
    'JR006342':'020003',
    'JR027308':'020003',
    'JR005298':'020003',
    'JR006343':'020003',
    'JR006843':'020003',
    'JR006829':'020003',
    'JR005822':'020003',
    'JR006835':'020003',
    'JR027319':'020003',
    'JR005127':'020003',
    'JR083533':'020003',
    'JR006789':'020003',
    'JR006790':'020003',
    'JR041815':'020003',
    'JR083628':'020003',
    'JR083629':'020003',
    'JR083630':'020003',
    'JR058292':'020003',
    'JR068875':'020003',
    'JR081030':'020003',
    'JR076477':'020003',
    'JR005852':'020003',
    'JR005851':'020003',
    'JR083475':'020003',
    'JR007132':'020003',
    'JR012300':'020003',
    'JR013355':'020003',
    'JR013264':'020003',
    'JR012649':'020003',
    'JR013281':'020003',
    'JR012862':'020003',
    'JR013457':'020003',
    'JR004620':'020003',
    'JR007013':'020003',
    'JR011367':'020003',
    'JR014543':'020003',
    'JR017137':'020003',
    'JR018374':'020003',
    'JR019724':'020003',
    'JR024553':'020003',
    'JR024681':'020003',
    'JR024909':'020003',
    'JR026767':'020003',
    'JR027286':'020003',
    'JR048780':'020003',
    'JR056377':'020003',
    'JR078343':'020003',
    'JR079550':'020003',
    'JR079852':'020003',
    'JR080835':'020003',
    'JR082311':'020003',
    'JR090022':'020003',
    'JR090023':'020003',
    'JR100945':'020003',
    'JR107699':'020003',
    'JR109345':'020003',
    'JR109989':'020003',
    'JR128414':'020003',
    'JR128435':'020003',
    'JR128461':'020003',
    'JR128480':'020003',
    'JR128482':'020003',
    'JR128483':'020003',
    'JR130072':'020003',
    'JR133127':'020003',
    'JR134292':'020003',
    'JR134315':'020003',
    'JR134341':'020003',
    'JR134359':'020003',
    'JR134385':'020003',
    'JR136344':'020003',
    'JR136350':'020003',
    'JR136385':'020003',
    'JR136386':'020003',
    'JR138125':'020003',
    'JR138128':'020003',
    'JR138129':'020003',
    'JR138132':'020003',
    'JR138133':'020003',
    'JR138144':'020003',
    'JR141461':'020003',
    'JR141552':'020003',
    'JR147083':'020003',
    'JR147088':'020003',
    'JR147093':'020003',
    'JR147095':'020003',
    'JR147096':'020003',
    'JR147099':'020003',
    'JR147100':'020003',
    'JR147101':'020003',
    'JR147102':'020003',
    'JR147103':'020003',
    'JR147104':'020003',
    'JR147148':'020003',
    'JR147323':'020003',
    'JR147324':'020003',
    'JR147325':'020003',
    'JR147326':'020003',
    'JR147327':'020003',
    'JR147328':'020003',
    'JR147329':'020003',
    'JR147341':'020003',
    'JR153156':'020003',
    'JR153157':'020003',
    'JR157136':'020003',
    'JR157138':'020003',
    'JR157139':'020003',
    'JR157146':'020003',
    'JR157165':'020003',
    'JR157168':'020003',
    'JR157174':'020003',
    'JR157247':'020003',
    'JR157248':'020003',
    'JR157256':'020003',
    'JR157257':'020003',
    'JR157258':'020003',
    'JR157260':'020003',
    'JR157265':'020003',
    'JR157267':'020003',
    'JR157268':'020003',
    'JR157269':'020003',
    'JR157270':'020003',
    'JR157271':'020003',
    'JR157272':'020003',
    'JR157273':'020003',
    'JR157274':'020003',
    'JR157275':'020003',
    'JR157276':'020003',
    'JR157277':'020003',
    'JR157289':'020003',
    'JR157320':'020003',
    'JR157329':'020003',
    'JR157330':'020003',
    'JR157331':'020003',
    'JR157332':'020003',
    'JR157333':'020003',
    'JR157334':'020003',
    'JR157335':'020003',
    'JR157396':'020003',
    'JR157413':'020003',
    'JR157426':'020003',
    'JR157427':'020003',
    'JR157435':'020003',
    'JR157438':'020003',
    'JR157445':'020003',
    'JR157451':'020003',
    'JR157465':'020003',
    'JR157471':'020003',
    'JR157487':'020003',
    'JR157518':'020003',
    'JR174405':'020003',
    'JR174791':'020003'}


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
jr_list=['JR133164',
'JR003639',
'JR102014',
'JR042390',
'JR003262',
'JR077208',
'JR003313',
'JR003460',
'JR003412',
'JR003186',
'JR003300',
'JR003326',
'JR003181',
'JR003004',
'JR128440',
'JR083319',
'JR049056',
'JR003011',
'JR100794',
'JR003390',
'JR128448',
'JR002992',
'JR035519',
'JR111274',
'JR032321',
'JR003324',
'JR090171',
'JR003734',
'JR003735',
'JR003736',
'JR003221',
'JR003006',
'JR128421',
'JR003108',
'JR003050',
'JR003051',
'JR003052',
'JR003071',
'JR003524',
'JR128469',
'JR101506',
'JR112787',
'JR112788',
'JR090204',
'JR128432',
'JR128460',
'JR101026',
'JR133147',
'JR107978',
'JR107977',
'JR127476',
'JR128446',
'JR128577',
'JR099857',
'JR128398',
'JR128546',
'JR041422',
'JR059835',
'JR106854',
'JR133134',
'JR061681',
'JR028560',
'JR128407',
'JR059444',
'JR058771',
'JR035361',
'JR034572',
'JR042245',
'JR055460',
'JR080967',
'JR027169',
'JR057775',
'JR004160',
'JR127499',
'JR045240',
'JR054058',
'JR004138',
'JR069044',
'JR004154',
'JR003698',
'JR066783',
'JR128420',
'JR128436',
'JR003587',
'JR107458',
'JR128441',
'JR128422',
'JR003718',
'JR003793',
'JR003794',
'JR003795',
'JR003796',
'JR003797',
'JR003798',
'JR003799',
'JR003800',
'JR003801',
'JR003802',
'JR003778',
'JR128439',
'JR101329',
'JR106418',
'JR106417',
'JR174669',
'JR003859',
'JR128438',
'JR128529',
'JR029833',
'JR106416',
'JR101426',
'JR003925',
'JR003948',
'JR106191',
'JR106189',
'JR106192',
'JR071720',
'JR110571',
'JR133169',
'JR052812',
'JR101041',
'JR107488',
'JR106419',
'JR003959',
'JR004136',
'JR000008',
'JR128569',
'JR004255',
'JR004250',
'JR087576',
'JR128541',
'JR099811',
'JR174702',
'JR107511',
'JR090203',
'JR130381',
'JR112249',
'JR101100',
'JR004377',
'JR004323',
'JR133128',
'JR082589',
'JR115108',
'JR099825',
'JR046089',
'JR107238',
'JR114957',
'JR112233',
'JR122363',
'JR109910',
'JR112235',
'JR004523',
'JR012190',
'JR110077',
'JR012256',
'JR004646',
'JR084034',
'JR130379',
'JR100688',
'JR110179',
'JR004589',
'JR174828',
'JR108311',
'JR128090',
'JR036425',
'JR127968',
'JR004780',
'JR077413',
'JR012370',
'JR004722',
'JR067785',
'JR012055',
'JR098721',
'JR107019',
'JR098829',
'JR098710',
'JR012516',
'JR078422',
'JR051120',
'JR109400',
'JR174763',
'JR073084',
'JR100398',
'JR004622',
'JR074063',
'JR004750',
'JR084173',
'JR012431',
'JR073754',
'JR078248',
'JR012443',
'JR115526',
'JR055510',
'JR005763',
'JR066038',
'JR068431',
'JR174830',
'JR053706',
'JR004435',
'JR098824',
'JR107505',
'JR101740',
'JR100420',
'JR110684',
'JR000423',
'JR006389',
'JR005002',
'JR083221',
'JR075917',
'JR031734',
'JR084050',
'JR107820',
'JR100717',
'JR074077',
'JR078359',
'JR005148',
'JR013177',
'JR031742',
'JR058294',
'JR107194',
'JR054246',
'JR066642',
'JR122531',
'JR004645',
'JR083212',
'JR012845',
'JR004860',
'JR048541',
'JR111788',
'JR012311',
'JR083220',
'JR072382',
'JR110686',
'JR005105',
'JR109782',
'JR078166',
'JR005251',
'JR013255',
'JR107504',
'JR094857',
'JR094856',
'JR094855',
'JR107503',
'JR109784',
'JR038404',
'JR036295',
'JR115796',
'JR083587',
'JR111549',
'JR013814',
'JR013900',
'JR114857',
'JR014995',
'JR014922',
'JR064032',
'JR098660',
'JR053280',
'JR071533',
'JR115795',
'JR013632',
'JR014143',
'JR002575',
'JR013895',
'JR013896',
'JR115600',
'JR006781',
'JR005375',
'JR030096',
'JR008933',
'JR008936',
'JR008945',
'JR008944',
'JR008942',
'JR008947',
'JR008935',
'JR008938',
'JR008948',
'JR008943',
'JR008950',
'JR008941',
'JR008949',
'JR008937',
'JR008940',
'JR008934',
'JR008946',
'JR008952',
'JR008939',
'JR008930',
'JR008951',
'JR008931',
'JR008932',
'JR005410',
'JR005409',
'JR005439',
'JR005435',
'JR014819',
'JR069919',
'JR078485',
'JR005905',
'JR027383',
'JR005532',
'JR112328',
'JR006429',
'JR008162',
'JR005554',
'JR005926',
'JR056605',
'JR112252',
'JR000790',
'JR100084',
'JR100085',
'JR027548',
'JR006655',
'JR029009',
'JR007008',
'JR006178',
'JR069986',
'JR073844',
'JR077178',
'JR051031',
'JR005583',
'JR017791',
'JR006712',
'JR006793',
'JR060684',
'JR059757',
'JR032932',
'JR032053',
'JR006017',
'JR080898',
'JR069925',
'JR005920',
'JR005918',
'JR078733',
'JR063370',
'JR066084',
'JR006040',
'JR006794',
'JR009255',
'JR029170',
'JR115511',
'JR006257',
'JR015111',
'JR076176',
'JR006162',
'JR027447',
'JR027448',
'JR027449',
'JR080813',
'JR027493',
'JR006215',
'JR006216',
'JR006217',
'JR006218',
'JR006219',
'JR006220',
'JR080950',
'JR080955',
'JR007339',
'JR090010',
'JR006946',
'JR006947',
'JR006948',
'JR006342',
'JR027308',
'JR005298',
'JR006343',
'JR006843',
'JR006829',
'JR005822',
'JR006835',
'JR027319',
'JR005127',
'JR083533',
'JR006789',
'JR006790',
'JR041815',
'JR083628',
'JR083629',
'JR083630',
'JR058292',
'JR068875',
'JR081030',
'JR076477',
'JR005852',
'JR005851',
'JR083475',
'JR007132',
'JR012300',
'JR013355',
'JR013264',
'JR012649',
'JR013281',
'JR012862',
'JR013457',
'JR004620',
'JR007013',
'JR011367',
'JR014543',
'JR017137',
'JR018374',
'JR019724',
'JR024553',
'JR024681',
'JR024909',
'JR026767',
'JR027286',
'JR048780',
'JR056377',
'JR078343',
'JR079550',
'JR079852',
'JR080835',
'JR082311',
'JR090022',
'JR090023',
'JR100945',
'JR107699',
'JR109345',
'JR109989',
'JR128414',
'JR128435',
'JR128461',
'JR128480',
'JR128482',
'JR128483',
'JR130072',
'JR133127',
'JR134292',
'JR134315',
'JR134341',
'JR134359',
'JR134385',
'JR136344',
'JR136350',
'JR136385',
'JR136386',
'JR138125',
'JR138128',
'JR138129',
'JR138132',
'JR138133',
'JR138144',
'JR141461',
'JR141552',
'JR147083',
'JR147088',
'JR147093',
'JR147095',
'JR147096',
'JR147099',
'JR147100',
'JR147101',
'JR147102',
'JR147103',
'JR147104',
'JR147148',
'JR147323',
'JR147324',
'JR147325',
'JR147326',
'JR147327',
'JR147328',
'JR147329',
'JR147341',
'JR153156',
'JR153157',
'JR157136',
'JR157138',
'JR157139',
'JR157146',
'JR157165',
'JR157168',
'JR157174',
'JR157247',
'JR157248',
'JR157256',
'JR157257',
'JR157258',
'JR157260',
'JR157265',
'JR157267',
'JR157268',
'JR157269',
'JR157270',
'JR157271',
'JR157272',
'JR157273',
'JR157274',
'JR157275',
'JR157276',
'JR157277',
'JR157289',
'JR157320',
'JR157329',
'JR157330',
'JR157331',
'JR157332',
'JR157333',
'JR157334',
'JR157335',
'JR157396',
'JR157413',
'JR157426',
'JR157427',
'JR157435',
'JR157438',
'JR157445',
'JR157451',
'JR157465',
'JR157471',
'JR157487',
'JR157518',
'JR174405',
'JR174791']
for  JR in jr_list:
    yansuan(JR)
check()
print("DOWN"+'\n'+now2)

