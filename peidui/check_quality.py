import pandas as pd
from sqlalchemy import create_engine
import pickle
import datetime
import numpy as np


def _fast_csv(df, file_name):
    df.to_csv(file_name)


def _fast_pickle(df, file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(df, f)


def _muti_merge(df_list, iteration):
    """
    递归地将一列dataframe进行合并,外连接
    :param df_list: dataframe列表
    :param iteration: 迭代深度
    :return:
    合并后的dataframe
    """
    print(iteration)
    lenth = len(df_list)
    if lenth == 2:
        df_list = pd.merge(df_list[0], df_list[1], left_index=True, right_index=True, how='outer')
        return df_list
    elif lenth == 1:
        return df_list[0]
    elif lenth > 2:
        nt = round(lenth / 2)
        iteration += 1
        df_list = pd.merge(_muti_merge(df_list[:nt], iteration), _muti_merge(df_list[nt:], iteration), left_index=True,
                           right_index=True, how='outer')
        return df_list


def _muti_merge_diy(df_list, iteration, left, right):
    """
    递归地将一列dataframe进行合并,外连接
    :param df_list: dataframe列表
    :param iteration: 迭代深度
    :return:
    合并后的dataframe
    """
    print(iteration)
    lenth = len(df_list)
    if lenth == 2:
        df_list = pd.merge(df_list[0], df_list[1], how='outer', left_on=left, right_on=right)
        return df_list
    elif lenth == 1:
        return df_list[0]
    elif lenth > 2:
        nt = round(lenth / 2)
        iteration += 1
        left_df = _muti_merge_diy(df_list[:nt], iteration, left=left, right=right)
        right_df = _muti_merge_diy(df_list[nt:], iteration, left=left, right=right)
        df_list = pd.merge(left_df, right_df, left_on=left, right_on=right, how='outer')
        return df_list


def _get_all_nav(fund_id):
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_candidate = "select distinct source_id from fund_nv_data_source_copy2 where fund_id='{}'".format(fund_id)
    candidate = pd.read_sql_query(sql_candidate, engine)['source_id'].tolist()
    nav_list = []
    for ii in candidate:
        sql_nav = "select nav,statistic_date from fund_nv_data_source_copy2 where fund_id='{0}' and source_id='{1}' and is_used=1".format(
            fund_id, ii)
        nav = pd.read_sql_query(sql_nav, engine, index_col='statistic_date').rename(columns={'nav': ii})
        nav_list.append(nav)
    return _muti_merge(nav_list, 1)


def _complete_rate(df, foundation_date):
    data_num = len(df.dropna(how='any'))
    found_d = foundation_date['foundation_date'].tolist()[0]
    now_date = datetime.datetime.now().date()
    delt = (now_date - found_d).days
    return data_num / delt


def _cal_quality(df):
    source = df.columns.values.tolist()[0]
    df['shift'] = df.loc[:, source].shift(1)
    cf = df[df.shift == df[source]]
    if cf.empty:
        return 1
    else:
        return (1 - len(cf)) / len(df)


def _row_mode(ts, th):
    ts_list = ts.tolist()
    if np.isnan(ts_list[th]):
        return 'bad'
    cleaned_list = [x for x in ts_list if not np.isnan(x)]
    if len(cleaned_list) < 3:
        return 'bad'
    target = ts_list[th]
    mode_num = pd.Series(cleaned_list).mode().tolist()[0]
    if target == mode_num:
        return 'True'
    else:
        return 'False'


def _rate_mode(all_data, source_id):
    all_data = all_data.applymap(lambda x: x / 100 if x > 15 else x).applymap(lambda x: round(x, 4))
    th = all_data.columns.values.tolist().index(source_id)
    answer = all_data.apply(_row_mode, args=([th]), axis=1).tolist()
    nTrue = answer.count('True')
    nFalse = answer.count('False')
    if (nTrue + nFalse) == 0:
        return 1
    return nTrue / (nTrue + nFalse)


def _evaluate(source_id, source, fund_id, foundation_date):
    print("Getting nav")
    all_data = _get_all_nav(fund_id)
    if source not in all_data.columns.values.tolist():
        return {'源': source, '源id': source_id, '不重复率': np.nan, '覆盖率': np.nan, '众数率': np.nan}
    quality = {'源': source, '源id': source_id, '不重复率': _cal_quality(all_data.loc[:, [source]]),
               '覆盖率': _complete_rate(all_data.loc[:, [source]], foundation_date), '众数率': _rate_mode(all_data, source)}
    return quality


def _get_foundation(source_id):
    '''
    根据source_id找
    :param source_id:
    :return:
    '''
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')
    sql_com_get_fd = "select distinct bi.matched_id, cd.fund_id ,bfi.foundation_date from d_fund_nv as cd  \
    join base.id_match as bi on bi.source_id=cd.fund_id \
    join base.fund_info as bfi on bi.matched_id = bfi.fund_id \
    where cd.source_id='{}'".format(source_id)
    foundation_date = pd.read_sql_query(sql_com_get_fd, engine)
    return foundation_date


def _get_id_foundation():
    '''
    根据fund_id找
    :return:
    '''
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_candidate = "select distinct source from id_match where is_used=1 and matched_id like 'JR%%'"
    candidate = pd.read_sql_query(sql_candidate, engine)['source'].tolist()
    id_list = []
    for ii in candidate:
        print(ii)
        sql_com_get_match = "select distinct matched_id,source_id from id_match where source='{}' and is_used=1 and matched_id like 'JR%%'".format(
            ii)
        id_temp = pd.read_sql_query(sql_com_get_match, engine).rename(columns={'source_id': ii})
        if not id_temp.empty:
            id_list.append(id_temp)
    id_all = _muti_merge_diy(id_list, 1, 'matched_id', 'matched_id')
    sql_get_fd = "select distinct fund_id,foundation_date found from fund_info"
    date_all = pd.read_sql_query(sql_get_fd, engine)
    all_foundation = pd.merge(id_all, date_all, left_on='matched_id', right_on='fund_id', how='outer')
    return all_foundation


def fund_id_best(fund_id):
    engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
    sql_com = "select matched_id,source_id,source,foundation_date from id_match  left join fund_info on id_match.matched_id=fund_info.fund_id where  is_used=1 and matched_id = '{}'".format(
        fund_id)
    candidate_source = pd.read_sql_query(sql_com, engine)
    evaluate_list = []
    for i in range(len(candidate_source)):
        try:
            temp = _evaluate(candidate_source.loc[i, 'source_id'], candidate_source.loc[i, 'source'], fund_id,
                             candidate_source)
        except:
            return "没有净值数据", "没有净值数据"
        evaluate_list.append(temp)
    max_mark = 0
    best_source = 'None'
    best_source_id = 'None'
    for jj in evaluate_list:
        if jj['众数率'] < 0.9:
            continue
        temp_mark = jj['覆盖率'] * jj['不重复率']
        if temp_mark > max_mark:
            best_source = jj['源']
            best_source_id = jj['源id']
            max_mark = temp_mark
    return [best_source, best_source_id], evaluate_list


if __name__ == "__main__":
    # print("Getting date")
    # source='020008'
    # foundation_date=_get_foundation(source)
    # foundation_2=_get_id_foundation()
    test_1, test_2 = fund_id_best('JR132336')
    # result=_evaluate('HF00000012', source,foundation_date)


def turn_dict(lst):
    global key, value
    dic = {}
    if all([not isinstance(item, list) for item in lst]):
        if len(lst) == 2:
            key, value = lst
        elif len(lst) == 1:
            key = lst[0]
            value = ''
        elif len(lst) == 3:
            key = lst[0]
            value = lst[1]
        dic[key] = value
    else:
        for item in lst:
            subdic = turn_dict(item)
            dic.update(subdic)
    return dic
