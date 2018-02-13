from WindPy import w as wind
import re
import time
from New.engine import *

wind.start()    # 启动wind
now = time.strftime("%Y-%m-%d")
def jrbbb(jr):
    yy ="'XT1703499.XT','JR135238',	'XT1703378.XT','JR136323',	'XT1703377.XT','JR058300',	'XT1703375.XT','JR122820',	'XT1703264.XT','JR049705',	'XT1703262.XT','JR040919',	'XT1703182.XT','JR126682',	'XT1703070.XT','JR002194',	'XT1702907.XT','JR131724',	'XT1702906.XT','JR126710',	'XT1702905.XT','JR020944',	'XT1702776.XT','JR001403',	'XT1702709.XT','JR126360',	'XT1702606.XT','JR132113',	'XT1702605.XT','JR126669',	'XT1702604.XT','JR126636',	'XT1702603.XT','JR126638',	'XT1702601.XT','JR126998',	'XT1702600.XT','JR127004',	'XT1702599.XT','JR127005',	'XT1702598.XT','JR131917',	'XT1702461.XT','JR131815',	'XT1702455.XT','JR128119',	'XT1702452.XT','JR136259',	'XT1702349.XT','JR101947',	'XT1702344.XT','JR101954',	'XT1702274.XT','JR132029',	'XT1702273.XT','JR029247',	'XT1702258.XT','JR131780',	'XT1702249.XT','JR131754',	'XT1702248.XT','JR126478',	'XT1702228.XT','JR132189',	'XT1702227.XT','JR132190',	'XT1702226.XT','JR132191',	'XT1702225.XT','JR132192',	'XT1702224.XT','JR132193',	'XT1702223.XT','JR132194',	'XT1702222.XT','JR132195',	'XT1702174.XT','JR131838',	'XT1702146.XT','JR132177',	'XT1702113.XT','JR134563',	'XT1702012.XT','JR132185',	'XT1701960.XT','JR132221',	'XT1701959.XT','JR132222',	'XT1701899.XT','JR043956',	'XT1701847.XT','JR132186',	'XT1701815.XT','JR126562',	'XT1701814.XT','JR126442',	'XT1701786.XT','JR132213',	'XT1701731.XT','JR132215',	'XT1701698.XT','JR101951',	'XT1701691.XT','JR132214',	'XT1701498.XT','JR101950',	'XT1701154.XT','JR073347',	'XT1700828.XT','JR128135',	'XT1700697.XT','JR118653',	'XT1700618.XT','JR029899',	'XT1700294.XT','JR015847',	'XT1700249.XT','JR041459',	'XT1700120.XT','JR068530',	'XT1623847.XT','JR132196',	'XT1623554.XT','JR125796',	'XT1623386.XT','JR116547',	'XT1623286.XT','JR116589',	'XT1623158.XT','JR125840',	'XT1622941.XT','JR113824',	'XT1622776.XT','JR073511',	'XT1622764.XT','JR084324',	'XT1622122.XT','JR115408',	'XT1622062.XT','JR115578',	'XT1621773.XT','JR000797',	'XT1621631.XT','JR065550',	'XT1620582.XT','JR103587',	'XT1620312.XT','JR103204',	'XT1619594.XT','JR108594',	'XT1619418.XT','JR103298',	'XT1618352.XT','JR074575',	'XT1617384.XT','JR116102',	'XT1617247.XT','JR104643',	'XT1616417.XT','JR105052',	'XT1615601.XT','JR087048',	'XT1615364.XT','JR001260',	'XT1615214.XT','JR119390',	'XT1615213.XT','JR035874',	'XT1615212.XT','JR036435',	'XT1614258.XT','JR092151',	'XT1613828.XT','JR051081',	'XT1611709.XT','JR087127',	'XT1610612.XT','JR109365',	'XT1609628.XT','JR062545',	'XT1608951.XT','JR002320',	'XT1608950.XT','JR041339',	'XT1607512.XT','JR026903',	'XT1607427.XT','JR116380',	'XT1606781.XT','JR000526',	'XT1606160.XT','JR011122',	'XT1605966.XT','JR026200',	'XT1604822.XT','JR010904',	'XT1604180.XT','JR010903',	'XT1603670.XT','JR010655',	'XT1600738.XT','JR027830',	'XT1600692.XT','JR009760',	'XT1600476.XT','JR107450',	'XT1600414.XT','JR009421',	'XT1600413.XT','JR009420',	'XT1600412.XT','JR009419',	'XT1600411.XT','JR009418',	'XT1600410.XT','JR009417',	'XT1600409.XT','JR009416',	'XT1600408.XT','JR009415',	'XT1600407.XT','JR009414',	'XT1600406.XT','JR009413',	'XT1600405.XT','JR009412',	'XT1525337.XT','JR011083',	'XT1524968.XT','JR126271',	'XT1522783.XT','JR022444',	'XT1522350.XT','JR022385',	'XT1521929.XT','JR020288',	'XT1519863.XT','JR022269',	'XT1519278.XT','JR018950',	'XT1519072.XT','JR020384',	'XT1518369.XT','JR020643',	'XT1517794.XT','JR019083',	'XT1517586.XT','JR007242',	'XT1517561.XT','JR019124',	'XT1517263.XT','JR019646',	'XT1517221.XT','JR019256',	'XT1515811.XT','JR015323',	'XT1515581.XT','JR019349',	'XT1515440.XT','JR016357',	'XT1515324.XT','JR002175',	'XT1513445.XT','JR016251',	'XT1513444.XT','JR017082',	'XT1513353.XT','JR014481',	'XT1513304.XT','JR013684',	'XT1513280.XT','JR014275',	'XT1513261.XT','JR014199',	'XT1513249.XT','JR014200',	'XT1512441.XT','JR013298',	'XT1512014.XT','JR014458',	'XT1511967.XT','JR016358',	'XT1511852.XT','JR015442',	'XT1511548.XT','JR016088',	'XT1511072.XT','JR015471',	'XT1510999.XT','JR030565',	'XT1510648.XT','JR014228',	'XT1510413.XT','JR014934',	'XT1510001.XT','JR007052',	'XT1509966.XT','JR013850',	'XT1509529.XT','JR045951',	'XT1508962.XT','JR016502',	'XT1507419.XT','JR013774',	'XT1507236.XT','JR034917',	'XT1506866.XT','JR027330',	'XT1505930.XT','JR005469',	'XT1505805.XT','JR012667',	'XT1505483.XT','JR009930',	'XT1502896.XT','JR012985',	'XT1502703.XT','JR027296',	'XT1502335.XT','JR012762',	'XT1501436.XT','JR001174',	'XT1501413.XT','JR078633',	'XT1501316.XT','JR012836',	'XT1501269.XT','JR012778',	'XT1501232.XT','JR027284',	'XT1501144.XT','JR109419',	'XT1501143.XT','JR134483',	'XT1500941.XT','JR101000',	'XT149321.XT','JR022936',	'XT145167.XT','JR004463',	'XT145163.XT','JR131631',	'XT144700.XT','JR112536',	'XT143128.XT','JR048971',	'XT142961.XT','JR002609',	'XT1414339.XT','JR005236',	'XT1412185.XT','JR027394',	'XT1411346.XT','JR080054',	'XT141040.XT','JR027187',	'XT1410225.XT','JR018705',	'XT134338.XT','JR105182',	'XT134245.XT','JR005661',	'XT130923.XT','JR000048',	'XT125375.XT','JR003680',	'XT125374.XT','JR058618',	'XT125311.XT','JR082302',	'XT125269.XT','JR003746',	'XT123370.XT','JR004154',	'XT122359.XT','JR006874',	'XT1205897.XT','JR116035',	'XT115084.XT','JR018948',	'XT113162.XT','JR100579',	'XT112030.XT','JR100188',	'XT102636.XT','JR003655',	'XT102211.XT','JR121387',	'XT090745.XT','JR003110',	'XT090712.XT','JR003140',	'XT090384.XT','JR003097',	'XT080123.XT','JR099734',	'XT071130.XT','JR054799',	'XT070415.XT','JR001759',	'XT0300891.XT','JR078639',	'XT1702235.XT','JR123230',	'XT1702116.XT','JR132804',	'XT1701704.XT','JR043391',	'XT1701495.XT','JR126811',	'XT1700771.XT','JR126938',	'XT1700557.XT','JR132238',	'XT1623578.XT','JR117844',	'XT1621451.XT','JR132333',	'XT1621450.XT','JR132330',	'XT1620974.XT','JR103141',	'XT1620558.XT','JR116971',	'XT1618884.XT','JR116052',	'XT1616801.XT','JR105303',	'XT1616655.XT','JR119273',	'XT1616112.XT','JR132408',	'XT1611791.XT','JR075406',	'XT1611366.XT','JR098521',	'XT1606638.XT','JR132580',	'XT1606493.XT','JR099775',	'XT1606054.XT','JR060675',	'XT1606053.XT','JR054983',	'XT1606052.XT','JR049455',	'XT1606051.XT','JR077940',	'XT1600552.XT','JR100444',	'XT1525557.XT','JR079803',	'XT1520281.XT','JR099591',	'XT1520280.XT','JR078088',	'XT1520279.XT','JR099590',	'XT1517768.XT','JR027598',	'XT1517493.XT','JR101642',	'XT1516243.XT','JR128744',	'XT1515180.XT','JR100026',	'XT1512611.XT','JR073308',	'XT1511633.XT','JR027554',	'XT1511527.XT','JR118121',	'XT1509045.XT','JR027497',	'XT1508043.XT','JR101641',	'XT1506310.XT','JR100923',	'XT1501578.XT','JR005819',	'XT146866.XT','JR051689',	'XT146373.XT','JR012217',	'XT1411550.XT','JR099763',	'XT1410032.XT','JR099762',	'XT140108.XT','JR118746',	'XT135521.XT','JR044807',	'XT134420.XT','JR003961',	'XT131714.XT','JR099756',	'XT131241.XT','JR099755',	'XT131121.XT','JR101148',	'XT1306936.XT','JR034104',	'XT115236.XT','JR041724',	'XT101617.XT','JR079854',	'XT090908.XT','JR078054',	'XT070393.XT','JR003135',	'XT070222.XT','JR003134',"
    y=re.findall(r'{}\',\'(\w+\d+)\''.format(jr), yy)

    return y
def crawl_benchmark(id):    # 定义方法
    tmp = wind.wsd(id, "NAV_date,nav,NAV_acc", "ED", now, "")# 万得API用法
    df = pd.DataFrame(tmp.Data)   # 用结果(list)去构造一个DataFrame二维表
    df = df.T  # 把表转置
    print(df)
    ID1=jrbbb(id)
    ID=str(ID1[0])
    A=str(df.iloc[0,0])
    if A =="None":
        R="None"
        B = str(df.iloc[0, 1])
        C = str(df.iloc[0, 2])
        return (ID,R, B, C)
    else:
        r = re.findall(r"(.+?) ", A)
        R = r[0]
        B = str(df.iloc[0, 1])
        C = str(df.iloc[0, 2])
        return (ID,R, B, C)


# --------------------------------------------------------------------------------------------------------------------io文件


# ----------------------------------------------------------------------------------------------------------------------io文件
def crawl():
    wdid=('XT1703499.XT',	'XT1703378.XT',	'XT1703377.XT',	'XT1703375.XT',	'XT1703264.XT',	'XT1703262.XT',	'XT1703182.XT',	'XT1703070.XT',	'XT1702907.XT',	'XT1702906.XT',	'XT1702905.XT',	'XT1702776.XT',	'XT1702709.XT',	'XT1702606.XT',	'XT1702605.XT',	'XT1702604.XT',	'XT1702603.XT',	'XT1702601.XT',	'XT1702600.XT',	'XT1702599.XT',	'XT1702598.XT',	'XT1702461.XT',	'XT1702455.XT',	'XT1702452.XT',	'XT1702349.XT',	'XT1702344.XT',	'XT1702274.XT',	'XT1702273.XT',	'XT1702258.XT',	'XT1702249.XT',	'XT1702248.XT',	'XT1702228.XT',	'XT1702227.XT',	'XT1702226.XT',	'XT1702225.XT',	'XT1702224.XT',	'XT1702223.XT',	'XT1702222.XT',	'XT1702174.XT',	'XT1702146.XT',	'XT1702113.XT',	'XT1702012.XT',	'XT1701960.XT',	'XT1701959.XT',	'XT1701899.XT',	'XT1701847.XT',	'XT1701815.XT',	'XT1701814.XT',	'XT1701786.XT',	'XT1701731.XT',	'XT1701698.XT',	'XT1701691.XT',	'XT1701498.XT',	'XT1701154.XT',	'XT1700828.XT',	'XT1700697.XT',	'XT1700618.XT',	'XT1700294.XT',	'XT1700249.XT',	'XT1700120.XT',	'XT1623847.XT',	'XT1623554.XT',	'XT1623386.XT',	'XT1623286.XT',	'XT1623158.XT',	'XT1622941.XT',	'XT1622776.XT',	'XT1622764.XT',	'XT1622122.XT',	'XT1622062.XT',	'XT1621773.XT',	'XT1621631.XT',	'XT1620582.XT',	'XT1620312.XT',	'XT1619594.XT',	'XT1619418.XT',	'XT1618352.XT',	'XT1617384.XT',	'XT1617247.XT',	'XT1616417.XT',	'XT1615601.XT',	'XT1615364.XT',	'XT1615214.XT',	'XT1615213.XT',	'XT1615212.XT',	'XT1614258.XT',	'XT1613828.XT',	'XT1611709.XT',	'XT1610612.XT',	'XT1609628.XT',	'XT1608951.XT',	'XT1608950.XT',	'XT1607512.XT',	'XT1607427.XT',	'XT1606781.XT',	'XT1606160.XT',	'XT1605966.XT',	'XT1604822.XT',	'XT1604180.XT',	'XT1603670.XT',	'XT1600738.XT',	'XT1600692.XT',	'XT1600476.XT',	'XT1600414.XT',	'XT1600413.XT',	'XT1600412.XT',	'XT1600411.XT',	'XT1600410.XT',	'XT1600409.XT',	'XT1600408.XT',	'XT1600407.XT',	'XT1600406.XT',	'XT1600405.XT',	'XT1525337.XT',	'XT1524968.XT',	'XT1522783.XT',	'XT1522350.XT',	'XT1521929.XT',	'XT1519863.XT',	'XT1519278.XT',	'XT1519072.XT',	'XT1518369.XT',	'XT1517794.XT',	'XT1517586.XT',	'XT1517561.XT',	'XT1517263.XT',	'XT1517221.XT',	'XT1515811.XT',	'XT1515581.XT',	'XT1515440.XT',	'XT1515324.XT',	'XT1513445.XT',	'XT1513444.XT',	'XT1513353.XT',	'XT1513304.XT',	'XT1513280.XT',	'XT1513261.XT',	'XT1513249.XT',	'XT1512441.XT',	'XT1512014.XT',	'XT1511967.XT',	'XT1511852.XT',	'XT1511548.XT',	'XT1511072.XT',	'XT1510999.XT',	'XT1510648.XT',	'XT1510413.XT',	'XT1510001.XT',	'XT1509966.XT',	'XT1509529.XT',	'XT1508962.XT',	'XT1507419.XT',	'XT1507236.XT',	'XT1506866.XT',	'XT1505930.XT',	'XT1505805.XT',	'XT1505483.XT',	'XT1502896.XT',	'XT1502703.XT',	'XT1502335.XT',	'XT1501436.XT',	'XT1501413.XT',	'XT1501316.XT',	'XT1501269.XT',	'XT1501232.XT',	'XT1501144.XT',	'XT1501143.XT',	'XT1500941.XT',	'XT149321.XT',	'XT145167.XT',	'XT145163.XT',	'XT144700.XT',	'XT143128.XT',	'XT142961.XT',	'XT1414339.XT',	'XT1412185.XT',	'XT1411346.XT',	'XT141040.XT',	'XT1410225.XT',	'XT134338.XT',	'XT134245.XT',	'XT130923.XT',	'XT125375.XT',	'XT125374.XT',	'XT125311.XT',	'XT125269.XT',	'XT123370.XT',	'XT122359.XT',	'XT1205897.XT',	'XT115084.XT',	'XT113162.XT',	'XT112030.XT',	'XT102636.XT',	'XT102211.XT',	'XT090745.XT',	'XT090712.XT',	'XT090384.XT',	'XT080123.XT',	'XT071130.XT',	'XT070415.XT',	'XT0300891.XT',	'XT1702235.XT',	'XT1702116.XT',	'XT1701704.XT',	'XT1701495.XT',	'XT1700771.XT',	'XT1700557.XT',	'XT1623578.XT',	'XT1621451.XT',	'XT1621450.XT',	'XT1620974.XT',	'XT1620558.XT',	'XT1618884.XT',	'XT1616801.XT',	'XT1616655.XT',	'XT1616112.XT',	'XT1611791.XT',	'XT1611366.XT',	'XT1606638.XT',	'XT1606493.XT',	'XT1606054.XT',	'XT1606053.XT',	'XT1606052.XT',	'XT1606051.XT',	'XT1600552.XT',	'XT1525557.XT',	'XT1520281.XT',	'XT1520280.XT',	'XT1520279.XT',	'XT1517768.XT',	'XT1517493.XT',	'XT1516243.XT',	'XT1515180.XT',	'XT1512611.XT',	'XT1511633.XT',	'XT1511527.XT',	'XT1509045.XT',	'XT1508043.XT',	'XT1506310.XT',	'XT1501578.XT',	'XT146866.XT',	'XT146373.XT',	'XT1411550.XT',	'XT1410032.XT',	'XT140108.XT',	'XT135521.XT',	'XT134420.XT',	'XT131714.XT',	'XT131241.XT',	'XT131121.XT',	'XT1306936.XT',	'XT115236.XT',	'XT101617.XT',	'XT090908.XT',	'XT070393.XT',	'XT070222.XT')
    DF=[]
    for i in wdid:
        q=crawl_benchmark(i)
        print(q)
        if q[1] =="None":
            pass
        else:
            Q=(3,'第三方',13,'Wind')
            qq=q+Q

            DF.append(qq)
    print(DF)
    df = pd.DataFrame(DF)
    df.columns = ["fund_id","statistic_date", "nav", "added_nav", "source_code", "source", "data_source","data_source_name"]
    print(df)
    dataframe=df

    print(dataframe)
    # df = dataframe.iloc[:, [0, 1,2,3]]
    # df["source"]="020007"
    # df["is_used"]=1
    # df["is_del"]=0

    is_checked = input("输入1来确认入库\n")

    if is_checked == "1":


        to_sql("fund_nv_data_source", engine_base, dataframe, type="update")
        # to_sql("d_fund_nv", engine_crawl_private, dataframe, type="update")
    else:
        pass

def main():
    crawl()
    print(now2)

if __name__ == "__main__":
     main()
