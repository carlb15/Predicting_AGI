//Load in the uncleaned data
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

//uncleaned counts
var count11 = myrdd11.count()
var count12 = myrdd12.count()
var count13 = myrdd13.count()
var count14 = myrdd14.count()
var count15 = myrdd15.count()
var count16 = myrdd16.count()
var count17 = myrdd17.count()

//Get count of uncleaned dataset
val unclean_count = count11 + count12 + count13 + count14 + count15 + count16 + count17
println(unclean_count)

//Load in cleaned data
val path: String = "hdfs:///user/ajs811/bdad/proj/data/irs_total.txt"
var myrdd = sc.textFile(path)

//map array of words to each line
val newrdd = finalrdd.map(line => line.split(","))

//Split rdds into numeric and string
val numrdd = newrdd.map(line => (line(4).toInt, line(5).toDouble))

//Count number of records
val count = numrdd.count()
println(count)

//Calculate averages
val stub_avg = numrdd.map(_._1).mean()
val agi_avg = numrdd.map(_._2).mean()

//Calculate mins
val stub_min = numrdd.map(_._1).min()
val agi_min = numrdd.map(_._2).min()

//Calculate maxs
val stub_max = numrdd.map(_._1).max()
val agi_max = numrdd.map(_._2).max()

//Calculate range
val stub_rang = stub_max - stub_min
val agi_rang = agi_max - agi_min

//Develop string rdd
val strrdd = newrdd.map(line => (line(0), line(1), line(2), line(3), line(6)))

//Get lengths of each
val lenrdd = strrdd.map(line => (line._1.length, line._2.length, line._3.length, line._4.length, line._5.length))

//Get max length of each string
val statefips = lenrdd.map(_._1).max()
val state = lenrdd.map(_._2).max()
val countyfips = lenrdd.map(_._3).max()
val countyname = lenrdd.map(_._4).max()
val st_ct_fips = lenrdd.map(_._5).max()