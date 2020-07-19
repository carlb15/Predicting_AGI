//Load in data
val path11: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/11.csv"
val path12: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/12.csv"
val path13: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/13.csv"
val path14: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/14.csv"
val path15: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/15.csv"
val path16: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/16.csv"
val path17: String = "hdfs:///user/ajs811/bdad/proj/data/county_irs/17.csv"

var myrdd11 = sc.textFile(path11)
var myrdd12 = sc.textFile(path12)
var myrdd13 = sc.textFile(path13)
var myrdd14 = sc.textFile(path14)
var myrdd15 = sc.textFile(path15)
var myrdd16 = sc.textFile(path16)
var myrdd17 = sc.textFile(path17)

//Remove headers
myrdd11 = myrdd11.filter(line => !line.contains("STATEFIPS"))
myrdd12 = myrdd12.filter(line => !line.contains("STATEFIPS"))
myrdd13 = myrdd13.filter(line => !line.contains("STATEFIPS"))
myrdd14 = myrdd14.filter(line => !line.contains("STATEFIPS"))
myrdd15 = myrdd15.filter(line => !line.contains("STATEFIPS"))
myrdd16 = myrdd16.filter(line => !line.contains("STATEFIPS"))
myrdd17 = myrdd17.filter(line => !line.contains("STATEFIPS"))

//map array of words to each line
val newrdd11 = myrdd11.map(line => line.split(","))
val newrdd12 = myrdd12.map(line => line.split(","))
val newrdd13 = myrdd13.map(line => line.split(","))
val newrdd14_t = myrdd14.map(line => line.split(","))
//for 14, there should be 128 fields
val newrdd14 = newrdd14_t.filter(line => line.length == 128)
val newrdd15 = myrdd15.map(line => line.split(","))
val newrdd16 = myrdd16.map(line => line.split(","))
val newrdd17 = myrdd17.map(line => line.split(","))

//Only take certain columns - STATEFIPS (0), STATE (1), COUNTYFIPS (2), COUNTYNAME (3), agi_stub (4), A00100 ((11,10), (12,12), (13,12), (14,15), (15,19), (16,19), (17,21), STATEFIPS + COUNTYFIPS, year)
val condrdd11 = newrdd11.map(record => (record(0), record(1), record(2), record(3), record(4), record(10), record(0) + record(2), "2011"))
val condrdd12 = newrdd12.map(record => (record(0), record(1), record(2), record(3), record(4), record(12), record(0) + record(2), "2012"))
val condrdd13 = newrdd13.map(record => (record(0), record(1), record(2), record(3), record(4), record(12), record(0) + record(2), "2013"))
val condrdd14 = newrdd14.map(record => (record(0), record(1), record(2), record(3), record(4), record(15), record(0) + record(2), "2014"))
val condrdd15 = newrdd15.map(record => (record(0), record(1), record(2), record(3), record(4), record(19), record(0) + record(2), "2015"))
val condrdd16 = newrdd16.map(record => (record(0), record(1), record(2), record(3), record(4), record(19), record(0) + record(2), "2016"))
val condrdd17 = newrdd17.map(record => (record(0), record(1), record(2), record(3), record(4), record(21), record(0) + record(2), "2017"))

//Export files to textfiles

//Combine rdds
val condrdd = condrdd11 ++ condrdd12 ++ condrdd13 ++ condrdd16 ++ condrdd17 ++ condrdd15  ++ condrdd14 

//Convert tuple to string
val strRDD = condrdd.map(tup => tup.toString)

//Remove "(" and ")"
val finalrdd = strRDD.map(line => line.replace("(","").replace(")", ""))

//Export finalrdd to textfile --> for purposes of cleaning assignment
//Save to textFile
val baseDir: String = "hdfs:///user/ajs811/bdad/proj/data/irs_total"
//val baseDir: String = "hdfs://user/mlu216/SHARE/IRS/irs_total"
finalrdd.saveAsTextFile(baseDir)

//Create dataframe --> to transfer data to rest of team
val df = spark.createDataFrame(condrdd).toDF("fipstate", "STATE", "fipscty", "COUNTYNAME", "agi_stub", "agi", "fipstate_fipscty", "year")

df.write.parquet("hdfs:///user/mlu216/SHARE/IRS/county_irs.parquet")



