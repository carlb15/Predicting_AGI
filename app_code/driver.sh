#!/bin/bash

parameters=('gender' 'race' 'business' 'education')
settings=(0 0 0 0)
declare -i idx=0
while [ $idx -lt 4 ]
do
	echo "Include ${parameters[$idx]} data? Y/n: "
	read input
	if [ $input = 'y' ] || [ $input = 'Y' ] || [ $input = 'n' ] || [ $input = 'N' ]
	then
		if [ $input = 'y' ] || [ $input = 'Y' ]
		then
			settings[$idx]=1
		fi
		idx+=1
	else
		echo 'Only enter y/n. '
	fi
done
read MM DD Time<<<$(date +'%m %d %T')
echo "Input partition year between 2011 - 2017: "
read year
echo "Input seed to use: "
read seed
spark2-submit --name ModelTrainer --class ModelTrainer --master yarn --deploy-mode cluster --verbose target/scala-2.11/modeltrainer3_2.11-0.1.0-SNAPSHOT.jar /user/mlu216/SHARE/DF/combinedAvgAgiAndEdu.parquet /user/mlu216/SHARE/OUTPUT/${MM}/${DD}/${Time//:} $year $seed ${settings[@]}
