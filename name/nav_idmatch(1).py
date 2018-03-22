import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pickle
import xlwt
import re
from multiprocessing.dummy import Pool as ThreadPool


def source_df(source):
    pattern_02 = re.compile(r'02\d+')
    pattern_03 = re.compile(r'03\d+')
    pattern_04 = re.compile(r'04\d+')
    pattern_05 = re.compile(r'05\d+')
    if re.match(pattern_02,source):
        return 'd_fund_nv'
    elif re.match(pattern_03,source):
        return 's_fund_nv'
    elif re.match(pattern_04,source):
        return 't_fund_nv'
    elif re.match(pattern_05,source):
        return 'g_fund_nv'
    else :
        print('Not matched!')
        return np.nan

def nav_match(fund_id1,fund_id2,source_1,engine=create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')):
    """
    根据比较fund_id1,fund_id2同期的净值数据，判断是否相同

    Args:
    fund_id1 - str , fund_id2 - str , engine - create_engine.Engine

    Returns:
    is_match - bool

    """
    df1=source_df(source_1)
    sql_com = "SELECT b.statistic_date,  a.fund_id, b.fund_id,a.nav ,b.nav FROM ( SELECT  fund_id,   statistic_date,   nav FROM  "+df1+" WHERE fund_id ='"+fund_id1+"' ORDER BY statistic_date DESC) as a ,  (SELECT  fund_id,   statistic_date,  nav FROM  base.fund_nv_data_standard WHERE  fund_id ='"+fund_id2+"' ORDER BY statistic_date DESC) as b where a.statistic_date=b.statistic_date limit 5;"
    try:
        df=pd.read_sql_query(sql_com,engine)
    except:
        return False
    if df.empty:
        return 'Empty' #空表这次认为是真
    is_match=False
    try:
        for indexs in df.index:
            if abs((df.loc[indexs].values[3]-df.loc[indexs].values[4]))<0.001:
                is_match=True
            else:
                is_match=False
                break
    except:
        return 'Match_Error'
    return is_match

def init_candidate():
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com="SELECT \
	matched_id, \
	count(source) as num_source \
FROM \
	( \
		SELECT DISTINCT \
			matched_id, \
			source, \
            source_id \
		FROM \
			id_match \
		WHERE \
			is_used = 1 \
		And id_type = 1 \
		AND source IN ( \
			SELECT DISTINCT \
				source \
			FROM \
				id_match \
			WHERE \
				( \
					source LIKE '02%%' \
					OR source LIKE '03%%' \
					OR source LIKE '04%%' \
					OR source LIKE '05%%' \
				) \
			AND source <> '02' \
			AND source <> '03' \
			AND source <> '04' \
			AND source <> '05' \
		) \
	) as a \
GROUP BY \
	a.matched_id \
HAVING \
	count(source) > 1"
    df = pd.read_sql_query(sql_com, engine)
    return df

def get_all_source(matched_id):
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com="SELECT DISTINCT matched_id, source, source_id FROM  id_match WHERE is_used=1 and matched_id = '"+matched_id+"' AND source IN (SELECT DISTINCT source  FROM  id_match WHERE (source LIKE '02%%' OR source LIKE '03%%' OR source LIKE '04%%' OR source LIKE '05%%') AND source <> '02' AND source <> '03' AND source <> '04' AND source <> '05')"
    df = pd.read_sql_query(sql_com, engine)
    return df

def muti_task(i):
    print(i)
    row_list = []
    matched_id = candidate.loc[i, 'matched_id']
    row_list.append(str(matched_id))
    all_source = get_all_source(matched_id)
    count_source = candidate.loc[i, 'num_source']
    row_list.append(str(count_source))
    candidate_source = []  # 备选源表
    candidate_source_id = []  # 备选源表
    for j in range(count_source):
        candidate_source.append(all_source.loc[j, 'source'])  # 数值格式
        candidate_source_id.append(all_source.loc[j, 'source_id'])
        row_list.append(str(all_source.loc[j, 'source']))  # 字符串格式
        row_list.append(str(all_source.loc[j, 'source_id']))  # 字符串格式
    row_list.append('右方为净值匹配结果')
    for i in range(count_source):
        row_list.append(candidate_source[i] + '源的' + candidate_source_id[i])
        row_list.append(nav_match(candidate_source_id[i], matched_id, candidate_source[i]))
    return row_list

if __name__ == "__main__":
    candidate=init_candidate()
    num_back=len(candidate)
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('My Sheet')
    all_list=[]
    pool = ThreadPool(8)
    all_list = pool.map(muti_task, range(0,1500))

    for i in range(len(all_list)):
        for j in range(len(all_list[i])):
            worksheet.write(i,j,all_list[i][j])
    workbook.save('10000-11000.xls')