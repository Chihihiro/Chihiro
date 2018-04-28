import numpy as np
import matplotlib.pyplot as plt

N = len(xs)
# menMeans = (20, 35, 30, 35, 27)
# menStd = (2, 3, 4, 1, 2)

ind = np.arange(N)  # the x locations for the groups
width = 0.5       # the width of the bars
size = 5
fig, ax = plt.subplots()
color2="lightblue"
rects1 = ax.bar(ind, ys, width, color=color2 ) #yerr=menStd)

# womenMeans = (25, 32, 34, 20, 25)
# womenStd = (3, 5, 2, 3, 3)

color="Khaki"
rects2 = ax.bar(ind + width, ys2, width, color=color)

# add some text for labels, title and axes ticks
ax.set_ylabel('percentage',fontsize=10)
ax.set_title(T1+' for improvement',fontsize=10)
ax.set_xticks(ind + width)
ax.set_xticklabels(xs)

ax.legend((rects1[0], rects2[0]), ('new', 'old'))

plt.xticks(fontsize=7)
plt.yticks(fontsize=7)
def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2, 1.005*height,
                '%d' % int(height),
                ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)

plt.show()