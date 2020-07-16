//Make folder in dumbo
mkdir data

//Move NAICS files from local to dumbo
scp ./county_irs.zip ajs811@dumbo.es.its.nyu.edu:/home/ajs811/bdad_proj/data

//Make folders in HDFS
hdfs dfs -mkdir -p bdad/proj/data

//Unzip data and move to HDFS folders
unzip county_irs.zip
hdfs dfs -put county_irs bdad/proj/data/