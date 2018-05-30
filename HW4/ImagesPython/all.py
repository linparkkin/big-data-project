import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# evenly sampled time at 200ms intervals
data = pd.read_csv('logsall.txt', header = None,sep=' ', dtype=float)
labels=['(4,2)','(8,2)','(8,4)','(8,8)','(16,4)','(16,8)','(32,4)','(32,8)','(64,4)','(64,8)']
a=np.array(data.values)
b=np.arange( a[:,1].size)
fig, ax = plt.subplots(nrows=3, ncols=1)
ax0, ax1, ax2 = ax.flatten()
width=0.5
ax0.bar(b, a[:,4],width,color ='r', label='coreset construction')
ax0.set_ylabel('time [ms]')
ax0.set_xlabel('(total cores, cores per executor)')
ax1.bar(b, a[:,5],width,color ='g', label='sequential algorithm')
ax1.set_ylabel('time [ms]')
ax1.set_xlabel('(total cores, cores per executor)')
ax2.bar(b, a[:,6],width,color ='b', label='load and count file')
ax2.set_ylabel('time [ms]')
ax2.set_xlabel('(total cores, cores per executor)')
ax0.legend(loc=2, shadow=True)
ax1.legend(loc=2, shadow=True)
ax2.legend(loc=2, shadow=True)
plt.sca(ax[0])
plt.xticks(b,labels);
plt.sca(ax[1])
plt.xticks(b,labels);
plt.sca(ax[2])
plt.xticks(b,labels);

plt.show()
