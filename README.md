# BDAD_proj

.
├── README.md
├── act_rem_code
├── app_code
│   ├── build.sbt
│   ├── driver.sh
│   ├── project
│   ├── src
│   │   └── main
│   │       └── scala
│   │           └── ModelTrainer.scala
    Build: sbt package
    Run: ./driver.sh
    Description: driver.sh is a bash file that upon execution will query the user for several inputs,
		then will sumbit a Spark job with correct parameters. It is assumed to be running on
		NYU DUMBO cluster.
    Input Dir: hdfs://user/mlu216/SHARE/DF/combinedAvgAgiAndEdu.parquet
    Output Dir: hdfs://user/mlu216/SHARE/OUTPUT/${Month job is submitted}/${Day job is submitted}/
		Job will output 4 files named with the time job was submitted, one .model file and 3
		.parquet files.
    Notes: Additional files (.class, etc.) are included with build as proof of originality of code.
	   See screenshots for an example runtime.
│   ├── tableau
│   │   ├── file_convert_for_tableau
│   │   └── read_outputs.scala
│   └── target
│       ├── scala-2.11
│       │   ├── modeltrainer3_2.11-0.1.0-SNAPSHOT.jar
│       │   └── resolution-cache
│       └── streams
|
├── data_ingest
│   ├── business\ patterns
│   │   └── README.txt
│   ├── education
│   │   └── README.txt
│   ├── irs
│   │   ├── data_ingest_2.0.scala
│   │   └── data_injest_explain.txt
│   └── us_census
│       ├── data+ingest
│       └── us-census.png
├── etl_code
│   ├── business
│   │   ├── BusinessProfile.scala
│   │   └── CbpCleaner.spark
	Notes: Run using spark2-shell -i CbpCleaner.spark, with BusinessProfile.scala in same folder
		Change directory specified in .spark code to load data at different path
│   ├── education
│   │   └── EduCleaner.spark
	Notes: Run using spark2-shell -i EduCleaner.spark
		Change directory specified in .spark code to load data at different path
│   ├── irs
│   │   ├── Cleaner.scalaspark
│   │   ├── IrsCleaner.spark
│   │   └── cleaner_explain.txt
│   ├── joinScript.spark
	Notes: Change directory specified for each dataset as needed
│   └── us_census
│       └── Cleaner.scalaspark
	Notes: Change directory specified for each dataset as needed
├── out.txt
├── profiling_code
│   ├── business
│   │   └── Profiling.scalaspark
	Note: Change directory specified if data resides at a different path
│   ├── irs
│   │   ├── Profiling.scalaspark
│   │   └── profiler_explain.txt
	Note: Change directory specified for each dataset as needed
│   └── us_census
│       └── Profiling.scalaspark
	Note: Change directory specified for each dataset as needed
├── screenshots
│   ├── exampleRuntime1.jpg
│   ├── exampleRuntime2.pdf
│   ├── exampleRuntime3.jpg
│   ├── exampleRuntime4.jpg
│   └── exampleRuntime5.jpg
├── test_code
│   ├── EduProfile.scala
│   ├── FINAL_IRS_CLEAN
│   ├── Hello
│   │   ├── build.sbt
│   │   ├── project
│   │   ├── src
│   │   │   └── main
│   │   │       └── scala
│   │   │           └── SimpleApp.scala
│   │   └── target
│   ├── IRS_NEWER_CLEAN
│   ├── IRS_NEW_CLEAN
│   ├── IrsProfile.scala
│   ├── JOIN_NEW_SCRIPT.spark
│   ├── JustInCase.spark
│   ├── MakeLabeledPoint.scala
│   ├── MakeLabeledPoint2.scala
│   ├── ModelTrainer
│   │   ├── build.sbt
│   │   ├── project
│   │   ├── src
│   │   │   └── main
│   │   │       └── scala
│   │   │           └── ModelTrainer.scala
│   │   └── target
│   │       ├── scala-2.11
│   │       │   ├── modeltrainer_2.11-0.1.0-SNAPSHOT.jar
│   ├── ModelTrainer2
│   │   ├── src
│   │   │   └── main
│   │   │       └── scala
│   │   │           └── ModelTrainer.scala
│   │   └── target
│   │       ├── scala-2.11
│   │       │   ├── modeltrainer2_2.11-0.1.0-SNAPSHOT.jar
│   ├── Model_training_example.spark
│   ├── Profile.spark
│   ├── SimpleApp.scala
│   ├── Test.spark
│   ├── almost_there
│   ├── first_attempt_at_agi_model
│   ├── session_2
│   └── test_session
