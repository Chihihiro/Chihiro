from engine import *

def _muti_merge(df_list, iteration):
    """
    递归地将一列dataframe进行合并
    :param df_list: dataframe列表
    :param iteration: 迭代深度
    :return:
    how = inner outer left right
    合并后的dataframe
    """
    print(iteration)
    lenth = len(df_list)
    if lenth == 2:
        df_list = pd.merge(df_list[0], df_list[1], how='outer',on = 'statistic_date')
        return df_list
    elif lenth == 1:
        return df_list[0]
    elif lenth > 2:
        nt = round(lenth / 2)
        iteration += 1
        df_list = pd.merge(_muti_merge(df_list[:nt], iteration), _muti_merge(df_list[nt:], iteration),  how='outer',on = 'statistic_date')
        return df_list



fund_id='JR000031'


df = pd.read_sql("select statistic_date,nav,source_id,is_used from \
 fund_nv_data_source_copy2 where fund_id ='{fid}'and source_id not IN ('03','04','05')".format(fid=fund_id), engine_base)

df2 = pd.read_sql("SELECT source_id FROM sync_source where "
                  "pk = '{fid}' and is_used=1".format(fid=fund_id), engine_config_private)

sync = to_list(df2["source_id"])
data = df["source_id"]
col = data.drop_duplicates()
source = to_list(col)
dff = []
for i in source:
    df1 = df[df["source_id"] == i]
    dff.append(df1)

a = _muti_merge(dff, 1)

num = int((len(a.columns)-1)/3)


column = to_list(a.columns)

los = []
for i in range(len(column)):
    los.append(i)

for i in range(len(source)):
    num = i*3+1
    num2 = i*3+2
    los.remove(num2)
    col_t = source[i]
    column[num] = col_t
    if col_t in sync:
        column[num+2] = 1
    else:
        column[num + 2] = 0


a.columns = column
dataframe = a.iloc[:,los]






