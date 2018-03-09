from engine import *


def nav_match(fund_id1,fund_id2):
    """
    根据比较fund_id1,fund_id2同期的净值数据，判断是否相同

    Args:
    fund_id1 - str , fund_id2 - str , engine - create_engine.Engine

    Returns:
    is_match - bool

    """
    sql_com = "SELECT b.statistic_date, a.statistic_date, a.fund_id, b.data_source,b.nav ,a.nav FROM ( SELECT  fund_id,  fund_name,  statistic_date,  data_source,   nav FROM  fund_nv_data_source WHERE fund_id ='"+fund_id1+"' ORDER BY statistic_date DESC) as a ,  (SELECT  fund_id,  fund_name,  statistic_date,  data_source,  nav FROM  fund_nv_data_source WHERE  fund_id ='"+fund_id2+"' ORDER BY statistic_date DESC) as b where a.statistic_date=b.statistic_date limit 5;"
    try:
        df=pd.read_sql_query(sql_com,engine)
    except:
        return False
    if df.empty:
        return 'Empty' #空表这次认为是真
    is_match=False
    for indexs in df.index:
        if abs((df.loc[indexs].values[4]-df.loc[indexs].values[5]))<0.0001:
            is_match=True
        else:
            is_match=False
            break
    return is_match