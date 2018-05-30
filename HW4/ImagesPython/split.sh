#!/bin/bash

for var in $(ls ~/IdeaProjects/big-data-project/HW4/Logs/DifferentK/ ); do
	totcore=$(echo "$var" | cut -d'_' -f1)
	core=$(echo "$var" | cut -d'_' -f2)
	distance=$(strings ~/IdeaProjects/big-data-project/HW4/Logs/DifferentK/$var | grep diversity | cut -d':' -f2)
	k=$(strings ~/IdeaProjects/big-data-project/HW4/Logs/DifferentK/$var | grep numBlocks | cut -d':' -f 3 | cut -d',' -f1)
	blocks=$(strings ~/IdeaProjects/big-data-project/HW4/Logs/DifferentK/$var | grep numBlocks | cut -d':' -f 4 )
	timing=$(strings ~/IdeaProjects/big-data-project/HW4/Logs/DifferentK/$var | grep ms | cut -d':' -f 2 | cut -d' ' -f 2)
	echo $totcore' '$core' '$distance' '$k' '$blocks' '$timing >> ./LogstestAllDiffk.txt
done
