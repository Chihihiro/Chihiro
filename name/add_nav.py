import pandas as pd
from sqlalchemy import create_engine
import pickle
import datetime
import numpy as np
import re

class _nav_check:
    def _muti_merge(self,df_list, iteration):
        """
        递归地将一列dataframe进行合并,外连接
        :param df_list: dataframe列表
        :param iteration: 迭代深度
        :return:
        合并后的dataframe
        """
        lenth = len(df_list)
        if lenth == 2:
            df_list = pd.merge(df_list[0], df_list[1], left_index=True, right_index=True, how='outer')
            return df_list
        elif lenth == 1:
            return df_list[0]
        elif lenth > 2:
            nt = round(lenth / 2)
            iteration += 1
            df_list = pd.merge(self._muti_merge(df_list[:nt], iteration), self._muti_merge(df_list[nt:], iteration),
                               left_index=True,
                               right_index=True, how='outer')
            return df_list

    def __init__(self, source,source_id):
        self.source=source
        self.source_id=source_id
        self.evaluate_list = []
        self.mark_list = []

    def _complete_rate(self,df, foundation_date):
        data_num = len(df.dropna(how='any'))
        found_d = foundation_date['foundation_date'].tolist()[0]
        now_date = datetime.datetime.now().date()
        delt = (now_date - found_d).days
        return round(data_num / delt,6)

    def _evaluate(self,source_id, source, foundation_date, all_data):
        if source + '@' + source_id not in all_data.columns.values.tolist():
            return {'基金id':self.fund_id ,'源': source, '源id': source_id, '不重复率': np.nan, '覆盖率': np.nan, '众数率': np.nan}
        quality = {'基金id':self.fund_id ,'源': source, '源id': source_id, '不重复率': self._cal_quality(all_data.loc[:, [source + '@' + source_id]]),
                   '覆盖率': self._complete_rate(all_data.loc[:, [source + '@' + source_id]], foundation_date),
                   '众数率': self._rate_mode(all_data, source, source_id)}
        return quality

    def _cal_quality(self,df):
        source = df.columns.values.tolist()[0]
        all_num=len(df)
        df['shift'] = df.loc[:, source].shift(1)
        cf = df[df['shift'] == df[source]]
        if cf.empty:
            return 1
        df[source][df['shift'] == df[source]]=np.nan
        df.dropna(inplace=True,how='any')
        return round(len(df) /all_num,6)

    def _row_mode(self,ts, th):
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

    def _rate_mode(self,all_data, source, source_id):
        all_data = all_data.applymap(lambda x: x / 100 if x > 15 else x).applymap(lambda x: round(x, 4))
        th = all_data.columns.values.tolist().index(source + '@' + source_id)
        answer = all_data.apply(self._row_mode, args=([th]), axis=1).tolist()
        nTrue = answer.count('True')
        nFalse = answer.count('False')
        if (nTrue + nFalse) == 0:
            return 1
        return round(nTrue / (nTrue + nFalse),6)

class sorce_check(_nav_check):
    def _source_pri(self,source):
        '''
        导航到指定源表
        :param source: 源ID
        :return:表名
        '''
        pattern_02 = re.compile(r'02\d+')
        pattern_03 = re.compile(r'03\d+')
        pattern_04 = re.compile(r'04\d+')
        pattern_05 = re.compile(r'05\d+')
        if re.match(pattern_02, source):
            return 'd_fund_nv'
        elif re.match(pattern_03, source):
            return 's_fund_nv'
        elif re.match(pattern_04, source):
            return 't_fund_nv'
        elif re.match(pattern_05, source):
            return 'g_fund_nv'
        else:
            print('Not matched!')
            return np.nan

    def _get_pri_nav(self, candidate_source):
        engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')
        candidate = candidate_source[candidate_source.matched_id == self.fund_id]
        can_id = candidate['source_id'].tolist()  # 源内基金id
        can_so = candidate['source'].tolist()  # 源id
        nav_list = []
        for ii, jj in zip(can_so, can_id):
            where = self._source_pri(ii)
            sql_nav = "select nav,statistic_date from {0} where fund_id='{1}' and source_id='{2}' and is_del=0".format(where, jj, ii)
            nav = pd.read_sql_query(sql_nav, engine, index_col='statistic_date').rename(columns={'nav': ii + '@' + jj})
            nav_list.append(nav)
        self.all_data=self._muti_merge(nav_list, 1)
        return self.all_data

    def _get_pri_addnav(self):
        engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/crawl_private?charset=utf8')
        where = self._source_pri(self.source)
        sql_nav = "select added_nav,statistic_date from {0} where fund_id='{1}' and source_id='{2}' and is_del=0".format(where, self.source_id, self.source)
        nav = pd.read_sql_query(sql_nav, engine, index_col='statistic_date').rename(columns={'added_nav':'source_addnav'})
        self.add_data=nav
        return self.add_data

    def _get_fund_id(self):
        engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
        sql_com = "select distinct matched_id,source_id,source from id_match  where source = '{}' and source_id='{}'".format(self.source,self.source_id)
        id_df = pd.read_sql_query(sql_com, engine)
        if id_df.empty:
            raise LookupError
        fund_id=id_df['matched_id'].tolist()[0]
        return fund_id

    def _get_pri_candidate(self):
        engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
        sql_com = "select matched_id,source_id,source,foundation_date from id_match  left join fund_info on id_match.matched_id=fund_info.fund_id where  source IN (SELECT DISTINCT source  FROM  base.id_match WHERE (source LIKE '02%%' OR source LIKE '03%%' OR source LIKE '04%%' OR source LIKE '05%%') AND source <> '02' AND source <> '03' AND source <> '04' AND source <> '05') and matched_id = '{}'".format(
            self.fund_id)
        candidate_source = pd.read_sql_query(sql_com, engine)
        return candidate_source

    def _get_standard_data(self):
        engine = create_engine('mysql+pymysql://jr_read_17:jr_read_17@182.254.128.241:4171/base?charset=utf8')
        sql_com="select nav, added_nav,statistic_date from fund_nv_data_standard where fund_id='{}'".format(self.fund_id)
        self.standard_nav = pd.read_sql_query(sql_com, engine).set_index('statistic_date')
        return self.standard_nav

    def __init__(self,source,source_id):
        self.source=source
        self.source_id=source_id
        self.fund_id=self._get_fund_id()
        self.is_new=False
        self.newdata=np.nan
        self.isdivided=False
        self.isavailable=False

    def _is_available(self):
        '''
        逻辑： 在众数率>0.8的前提下，如果没有分红拆分，那么可以不需要复权数据，直接同步数据
        如果存在分红拆分需要和标准表的累计净值数据比对，如果全部相同或基本相同（我设置90%以上相同）
        :return:
        '''
        if self.basic_info['众数率']>0.8:
            if not self.isdivided:
                is_available=True
            else:
                if self.add_data.empty or self.basic_info['累计净值占比']<=0.1:
                    is_available=False
                else:
                    temp=pd.merge(self.standard_nav[['added_nav']],self.add_data,left_index=True,right_index=True)
                    same_df=temp[temp.added_nav==temp.source_addnav]
                    if len(same_df)/len(temp)>0.9:
                        is_available=True
                    else:
                        is_available=False
        else:
            is_available=False
        return is_available

    def check_source(self):
        candidate=self._get_pri_candidate()
        all_data=self._get_pri_nav(candidate)
        sorce_res=self._evaluate(self.source_id,self.source,candidate,self.all_data)
        add_data=self._get_pri_addnav()
        if add_data.empty:
            sorce_res['累计净值占比']=0
        else:
            sorce_res['累计净值占比']=len(add_data.dropna(how='any'))/len(all_data[self.source+'@'+self.source_id].dropna())
        self.basic_info=sorce_res
        standard_nav=self._get_standard_data()
        self.isdivided=(not self.standard_nav['nav'].tolist()==self.standard_nav['added_nav'].tolist())
        self.isavailable=self._is_available()
        if self.isavailable:
            if self.isdivided:
                sorce_data = all_data[[self.source + '@' + self.source_id]].dropna()
                sorce_data['sorce_addnav']=sorce_data[self.source + '@' + self.source_id]
            else:
                sorce_data = pd.merge(all_data[[self.source + '@' + self.source_id]].dropna(), self.add_data.dropna(),
                                      left_index=True, right_index=True)
            t1=pd.merge(sorce_data,standard_nav,how='left',left_index=True,right_index=True)
            new_df=t1[pd.isnull(t1.nav)]
            if new_df.empty:
                self.is_new=False
            else:
                self.is_new=True
                self.newdata=new_df
        else:
            self.is_new=False
        source_status={'is_new':self.is_new,'is_available':self.isavailable,'is_divided':self.isdivided}

        return sorce_res,source_status,self.newdata




if __name__ == "__main__":
    t=sorce_check('020008','HF0000000C')
    t1,t2,t3=t.check_source()
    print(t.basic_info,t2)