import os
import re
import pandas as pd

pwd = os.getcwd()
dataFolder = 'Logs'
files = os.listdir(pwd + '/' + dataFolder)
files.sort()
outName = 'Metrics.txt'

out_file = open(outName, 'w')

for file in files:
	params = file.split('_', 5)

	totalCores = params[0]
	corePerMachine = params[1]
	dataSetSize = params[2]
	nCenters = params[3]
	partitions = params[4].split('.')[0]
	

	if(partitions == '32' and totalCores == '32' and corePerMachine == '4'):

		in_file = open(pwd + '/' + dataFolder + '/' + file,"r");
		in_file.readline()
		firstLine = in_file.readline()
		
		#check consistency name-computation

		secondLine = in_file.readline()
		diversityMeasure = re.split(' ', secondLine.split(':')[1])[1][:-1]

		thirdLine = in_file.readline()
		coresetTime = re.split(' ', thirdLine.split(':')[1])[1]

		fourthLine = in_file.readline()
		sequentialTime = re.split(' ', fourthLine.split(':')[1])[1]

		fifthLine = in_file.readline()
		loadTime = re.split(' ', fifthLine.split(':')[1])[1]

		out_file.write('{\"total_cores\":' + str(totalCores) + ', \"core_machine\":'+corePerMachine+
			', \"dataset_size\":\"' + dataSetSize + '\", \"n_centers\":' + nCenters + ', \"partitions\":' 
			+ partitions + ', \"diversity_measure\":' + str(diversityMeasure) + ', \"coreset_time\":'+str(coresetTime)+
			', \"sequential_time\":' + str(sequentialTime) + ', \"load_time\":' + str(loadTime) + '}\n' )

	
		in_file.close()
	
out_file.close()

# pd.DataFrame(data, columns=['total_cores', 'core_machine', 'dataset_size', 'n_centers', 'partitions', 'diversity_measure', 'coreset_time', 'sequential_time', 'load_time'])


# df = pd.read_json(out_file);
# print (df)

