#!/bin/bash

for var in $(ls ./Logs/ ); do
	totcore=$(echo "$var" | cut -d'_' -f1)
	core=$(echo "$var" | cut -d'_' -f2)
        dataset=$(echo "$var" | cut -d'_' -f3)
	k=$(strings ./Logs/$var | grep numBlocks | cut -d':' -f 3 | cut -d',' -f1)
	blocks=$(strings ./Logs/$var | grep numBlocks | cut -d':' -f 4 ) 
	timing=$(strings ./Logs/$var | grep ms | cut -d':' -f 2 | cut -d' ' -f 2) 
	echo $totcore' '$core' '$dataset' '$k' '$blocks' '$timing >> ./Logstest.txt
done



