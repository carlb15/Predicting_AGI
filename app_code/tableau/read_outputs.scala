//Copy and paste the below into spark

import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)


//Model summary performance
val path1 = "hdfs:///user/mlu216/SHARE/FINAL/summary.parquet"
val df1 = sqlCtx.parquetFile(path1)
df1.show(5)

//Model predictions (string county_id, float label (actual AGI), vector features, float prediction (predicted AGI), float goodness)
val path2 = "hdfs:///user/mlu216/SHARE/FINAL/pred.parquet"
val df2 = sqlCtx.parquetFile(path2)
df2.show(5)

//Training model coefficients (float coeff, string feature)

val path3 = "hdfs:///user/mlu216/SHARE/FINAL/coef.parquet"
val df3 = sqlCtx.parquetFile(path3)
df3.show(5)

//Save parquet files to csv in HDFS
df1.write.csv("bdad/proj/output/model_summary")
df2.select("county_id", "state", "county", "label", "prediction", "goodness").write.csv("bdad/proj/output/predictions_2017")
df3.write.csv("bdad/proj/output/coefficients_table")
//End of spark code

//Start of DUMBO code
//Exit Spark and move files from HDFS to local dumbo storage
cd bdad_proj/data/output
hdfs dfs -get bdad/proj/output/model_summary
hdfs dfs -get bdad/proj/output/predictions_2017
hdfs dfs -get bdad/proj/output/coefficients_table
//End of DUMBO code


//Start of local PC code
//Transfer files from Dumbo Storage to your PC

//Predictions
sftp ajs811@dumbo.es.its.nyu.edu:/home/ajs811/bdad_proj/data/output/predictions_2017
//ls to see all the part files associated with that csv
ls
//Get each of the part files to save locally
get part-.............csv

//Model Summary
sftp ajs811@dumbo.es.its.nyu.edu:/home/ajs811/bdad_proj/data/output/model_summary
ls
get part-....csv

//Coefficients
sftp ajs811@dumbo.es.its.nyu.edu:/home/ajs811/bdad_proj/data/output/coefficients_table
ls
get part-....csv
//End of local PC code


//Start of Excel instructions to combine/convert results to .xlsx
//For the model coefficients and predictions csv's, use excel to copy the two part files into one excel file (.xlsx) and add column headers shown below
county_id	state	county	label	prediction	goodness //predictions headers
coeff   feature //coefficients headers
//End of Excel Instructions

//Open Tableau templates and upload predictions and coefficients headers



