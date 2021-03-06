import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# evenly sampled time at 200ms intervals
data = pd.read_csv('logssamecorediffdataset.txt', header = None,sep=' ', dtype=float)
labels=['500.000','1.000.000','2.000.000','3.000.000','all']
a=np.array(data.values)
b=np.arange( a[:,3].size)
ffig, ax = plt.subplots()
width=0.2
ax.bar(b-width, a[:,4],width,color ='#EF9855', label='coreset construction')
ax.bar(b, a[:,5],width,color ='#85BFFC', label='sequential algorithm')
ax.bar(b+width, a[:,6],width,color ='#47C240', label='load and count file')
ax.legend(loc=4, shadow = True)
plt.xticks(b,labels);
plt.yscale('log')
ax.set_ylabel('time [ms]')
ax.set_xlabel('dataset')
plt.show()
