import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# all dataset
data = pd.read_csv('logsall.txt', header = None,sep=' ', dtype=float)
labels=['(4,2)','(8,2)','(8,4)','(8,8)','(16,4)','(16,8)','(32,4)','(32,8)','(64,4)','(64,8)']
a=np.array(data.values)
b=np.arange( a[:,1].size)
plt.subplot(131)
width=0.2
plt.bar(b-width, a[:,4],width,color ='#EF9855', label='coreset construction')
plt.bar(b, a[:,5],width,color ='#85BFFC', label='sequential algorithm')
plt.bar(b+width, a[:,6],width,color ='#47C240', label='load and count file')
plt.xticks(b,labels);
plt.yscale('log')
plt.ylabel('time [ms]')
plt.xlabel('(total cores, cores per executor)')

# partitions
data = pd.read_csv('logssamecorediffpart.txt', header = None,sep=' ', dtype=float)
a=np.array(data.values)
b=np.arange( a[:,3].size)
labels = [4,8,16,32,64,128,256]
plt.subplot(132)
width=0.2
plt.bar(b-width, a[:,4],width,color ='#EF9855', label='coreset construction')
plt.bar(b, a[:,5],width,color ='#85BFFC', label='sequential algorithm')
plt.bar(b+width, a[:,6],width,color ='#47C240', label='load and count file')
plt.xticks(b,labels);
plt.yscale('log')
plt.ylabel('time [ms]')
plt.xlabel('number of partitions')

# diff dataset
data = pd.read_csv('logssamecorediffdataset.txt', header = None,sep=' ', dtype=float)
labels=['500.000','1.000.000','2.000.000','3.000.000','all']
a=np.array(data.values)
b=np.arange( a[:,3].size)
plt.subplot(133)
width=0.2
plt.bar(b-width, a[:,4],width,color ='#EF9855', label='coreset construction')
plt.bar(b, a[:,5],width,color ='#85BFFC', label='sequential algorithm')
plt.bar(b+width, a[:,6],width,color ='#47C240', label='load and count file')
plt.legend(loc=1, shadow = True)
plt.xticks(b,labels);
plt.yscale('log')
plt.ylabel('time [ms]')
plt.xlabel('dataset')
plt.show()
