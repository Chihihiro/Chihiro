import numpy as np
from engine import *
import pylab as pl

x1 = [1, 2, 3, 4, 5]  # Make x, y arrays for each graph

y1 = [1, -4, 9, 16, 25]

x2 = [1, 2, 4, 6, 8]

y2 = [2, 4, 8, 12, 16]

pl.plot(x1, y1, 'c')  # use pylab to plot x and y
pl.plot(x2, y2, 'r')
pl.title('Plotofy vs.x')  # give plot a title
pl.xlabel('x axis')  # make axis labels
pl.ylabel('y axis')
pl.xlim(1.0, 9.0)  # set axis limits
pl.ylim(-10.0, 20.)
pl.grid(True)
# pl.show()  # show the plot on the screen
# pl.savefig('C:\\Users\\63220\\PycharmProjects\\QQX\\plot1.png', format='png')
# pl.plot(xData, yData1, color='b', linestyle='--', marker='o', label='y1 data')
# pl.plot(xData, yData2, color='r', linestyle='-', label='y2 data')
# pl.legend(loc='upper left')
pl.savefig('C:\\Users\\63220\\PycharmProjects\\QQX\\plot1.png'.format(now2))
pl.close()
pl.show()