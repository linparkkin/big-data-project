import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# evenly sampled time at 200ms intervals
data = pd.read_csv('LogstestAllDiffk.txt', header = None,sep=' ')
a=np.array(data.values)
b=np.arange( a[:,5].size)

labels = [4,8,16,32,64,128]
plt.subplot(121)
width=0.2
plt.bar(b-width, a[:,5],width,color ='#EF9855', label='coreset construction')
plt.bar(b+0, a[:,6],width,color ='#85BFFC', label='sequential algorithm')
plt.bar(b+width, a[:,7],width,color ='#47C240', label='load and count file')
plt.legend(loc=4, shadow = True)
plt.xticks(b,labels);
plt.yscale('log')
plt.ylabel('time [ms]')
plt.xlabel('number of clusters')

plt.subplot(122)
plt.plot(b, a[:,2],'bo-')
plt.ylabel('average distance')
plt.xlabel('number of clusters')
plt.xticks(b,labels);
plt.show()
