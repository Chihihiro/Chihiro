import random
import datetime
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from nv_test_1 import *
from mpl_toolkits.mplot3d import Axes3D

mpl.rcParams['font.size'] = 5.5

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')


#-------------------------------------------
p=[]
week=[]
for i in list1:
    a=i[0]
    b=i[1]
    p.append(b)
    week.append(a)

ww=[]
for i in week:
    a=i.replace('2017-','')
    b=a.replace('2018-','')
    d = b.replace('-', '')
    f=float(d)
    ww.append(d)
data=np.array(p)
wc=np.asarray(ww)
pp=np.asarray(p)


p_update=[]
# week=[]
for i in list_copy2:
    # a=i[0]
    b=i[1]
    p_update.append(b)
    # week.append(a)


p_2015=[]
# week=[]
for i in list_2015:
    # a=i[0]
    b=i[1]
    p_2015.append(b)
    # week.append(a)










#---------------------------------------------------------------------------------
from datetime import datetime
z=1
xs = wc
ys = pp
color =plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
ax.bar(xs, ys, zs=z, zdir='y', color=color, alpha=0.7)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys))

# dates = ['01/01/1991', '01/10/1991']
# xs = [datetime.strptime(d, '%m/%d/%Y').date() for d in dates]

z1=2
color =plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
color="lightblue"
ax.bar(xs, p_update, zs=z1, zdir='y', color=color, alpha=0.7)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(p_update))


z2=3
color =plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
# color="lightblue"
# color="lightyellow"
ax.bar(xs, p_2015, zs=z2, zdir='y', color=color, alpha=0.7)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(p_2015))




# z1=2
# xs1 = range(1,13)
# ys1 = 1000 * np.random.rand(12)
# color =plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
# ax.bar(xs1, ys1, zs=z1, zdir='y', color=color, alpha=0.8)
#  # ax.bar(xs, ys, zdir='y', color=color, alpha=0.8)
# ax.xaxis.set_major_locator(mpl.ticker.FixedLocator(xs1))
# ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys1))


ax.set_xlabel('Day')
ax.set_ylabel('2017,2016,2015')
ax.set_zlabel('statsicic fund nav')

plt.show()