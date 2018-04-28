from engine import *

import re


# df = df.applymap(lambda x: re.sub("\s|-|--|---", "", x) if type(x) is str else x)
# df = df.applymap(lambda x: None if (type(x) is str and x == "") else x)
def sub_wrong_to_none(x):
    if type(x) is str:
        s = re.sub("\s|--|---|-", "", x)
        if s == "":
            return None

    return x


def check_dataframe(dataframe):
    dataframe = dataframe.applymap(lambda x: sub_wrong_to_none(x))
    return dataframe


def diff(listA, listB):
    retA = [i for i in listA if i in listB]
    return retA


def pk(t_new, t_old, dkt={}):
    """

    :param t_new:
    :param t_old:
    :param dkt: {"old_col": "new_col"}
    :return:
    """

    sql_new = pd.read_sql("select * from {}".format(t_new), engine_base)
    sql_old = pd.read_sql("select * from {}".format(t_old), engine_base_test)

    df_new = check_dataframe(sql_new)
    df_old = check_dataframe(sql_old) #.rename(columns=dkt)


    cnt = df_new.count()
    total_num = len(sql_new)
    df_a = cnt.reset_index()
    df_a.columns = ["字段名字", "非空字段数量"]
    df_a["空字段数量"] = total_num - df_a["非空字段数量"]
    df_a["非空百分比"] = df_a["非空字段数量"] / total_num
    cnt_old = df_old.count()
    df_b = cnt_old.reset_index()
    u = len(sql_old)
    df_b.columns = ["字段名字", "old非空字段数量"]
    df_b["old空字段"] = u - df_b["old非空字段数量"]
    df_b["old非空百分比"] = df_b["old非空字段数量"] / u
    df_all = df_a.merge(df_b, on="字段名字", how="left", suffixes=("", "_"))
    df_all["改进后"] = df_all["非空百分比"].sub(df_all["old非空百分比"], 0)
    df_all["改进后"] = df_all["改进后"].apply(lambda x: '%.2f' % (x * 100))
    df_all["改进后"] = df_all["改进后"].apply(lambda x: str(x) + '%')
    df_all["非空字段数量"] = df_all["非空字段数量"].apply(lambda x: float('%.0f' % x))
    df_all["空字段数量"] = df_all["空字段数量"].apply(lambda x: '%.0f' % x)
    df_all["old非空百分比"] = df_all["old非空百分比"].apply(lambda x: '%.2f' % (x * 100))
    df_all["old非空百分比"] = df_all["old非空百分比"].apply(lambda x: str(x) + '%')
    df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: '%.2f' % (x * 100))
    df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: str(x) + '%')

    return df_all

#
# T1 = 'org_info'
# T2 = 'org_info_20180327'
# dict = {"employeescale": "employee_scale",
#         "total_fund_num": "fund_total_num"}

T1 = 'fund_info'
T2= 'fund_info_old_20171212'

# dict2 = {person_description.resume}

df_all = pk(T1, T2)
# to_table(df_all)


new = df_all.iloc[:, [0, 3, 6]]

new2 = new[new.iloc[:, 1] != new.iloc[:, 2]]

new2.iloc[:, 1] = new2.iloc[:, 1].apply(lambda x: float(re.sub("%", "", x)))
new2.iloc[:, 2] = new2.iloc[:, 2].apply(lambda x: float(re.sub("%", "", x)))

new3 = new2[(new2.iloc[:, 1] - new2.iloc[:, 2]) > 4]

x1 = new3.iloc[:, 0]
xs = to_list(x1)

y1 = new3.iloc[:, 2]
ys = to_list(y1)

y2 = new3.iloc[:, 1]
ys2 = to_list(y2)

# --------------------------------shunxui：
list_all = []
for i in range(len(ys)):
    l = [ys[i],ys2[i],xs[i]]
    list_all.append(l)
list_all.sort()
xs = []
ys = []
ys2 = []

for i in list_all:
    xs.append(i[2])
    ys.append(i[0])
    ys2.append(i[1])


# ------------------------------------------------------3D图

import random
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['font.size'] = 6.5
from mpl_toolkits.mplot3d import Axes3D

# fig = plt.figure()
fig = plt.figure(figsize=(8, 6))
ax = fig.add_subplot(111, projection='3d')

ax = fig.add_subplot(111, projection='3d')
z = 1
color = plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))

color = "Khaki"
# color = 'DeepSkyBlue'
ax.bar(xs, ys, zs=z, zdir='y', color=color, alpha=0.8)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys))
z1 = 2
color = plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
color = "lightblue"

# color='DodgerBlue'
ax.bar(xs, ys2, zs=z1, zdir='y', color=color, alpha=0.8)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys2))

# ax.set_xlabel('percentage', fontsize=11)
ax.set_ylabel('SMYT , WIND', fontsize=14)
ax.set_zlabel('A slow number', fontsize=12)
plt.show()
# ------------------------------------------------------------3图
xs=['04-11','04-04','03-28','03-16','03-22','03-20','03-09','02-27']
ys= [28,36,30,35,29,20,37,37]
ys2 = [43,60,45,53,39,34,56,23]

# ----------------------------------条状图
import numpy as np
import matplotlib.pyplot as plt

N = len(xs)
# menMeans = (20, 35, 30, 35, 27)
# menStd = (2, 3, 4, 1, 2)

ind = np.arange(N)  # the x locations for the groups
width = 0.4  # the width of the bars
size = 5
fig, ax = plt.subplots()
color = "lightblue"

color2 = "Khaki"
color3 = 'DodgerBlue'
color4 = 'DeepSkyBlue'
color6 = (0, 112, 192)
color7 = (0, 174, 239)
color8 = "Cyan"
rects1 = ax.bar(ind, ys2, width, color=color3)  # yerr=menStd)

# womenMeans = (25, 32, 34, 20, 25)
# womenStd = (3, 5, 2, 3, 3)


rects2 = ax.bar(ind + width, ys, width, color=color4)

# add some text for labels, title and axes ticks
# ax.set_ylabel('percentage', fontsize=10)
ax.set_title('Improvement of fund_info' , fontsize=10)
ax.set_xticks(ind + width)
ax.set_xticklabels(xs,fontsize=5)

ax.legend((rects1[0], rects2[0]), ('NEW', 'OLD'))

plt.xticks(fontsize=7)
plt.yticks(fontsize=7)


def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., 1.005 * height,
                '%d' % int(height),
                ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

# plt.legend(handles=[blue_line,red_line],loc='upper left')
plt.show()
