import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# evenly sampled time at 200ms intervals
data = pd.read_csv('logsall.txt', header = None,sep=' ', dtype=float)
labels=['(4,2)','(8,2)','(8,4)','(8,8)','(16,4)','(16,8)','(32,4)','(32,8)','(64,4)','(64,8)']
a=np.array(data.values)
b=np.arange( a[:,1].size)
fig, ax = plt.subplots()
width=0.2
ax.bar(b-width, a[:,4],width,color ='#EF9855', label='coreset construction')
ax.bar(b, a[:,5],width,color ='#85BFFC', label='sequential algorithm')
ax.bar(b+width, a[:,6],width,color ='#47C240', label='load and count file')
ax.legend(loc=7, shadow = True)
plt.xticks(b,labels);
plt.yscale('log')
ax.set_ylabel('time [ms]')
ax.set_xlabel('(total cores, cores per executor)')
plt.show()
