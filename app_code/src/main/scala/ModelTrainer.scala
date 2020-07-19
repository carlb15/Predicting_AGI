import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{rand,expr}

object ModelTrainer {
	def main(args: Array[String]) {
		if (args.length < 6) {
			System.err.println("Usage: ModelTrainer <input_file_path> <output_path> YR SEED G D B E")
			System.exit(1)
		}		

		val inputFilePath = args(0)
		val outputPath = args(1)
		val YEAR = args(2).toInt
		val SEED = args(3).toLong
		val includeFeatures = args.slice(4,8).map(_=="1")
		
		val spark = SparkSession.builder
			.appName("ModelTrainer")
			.getOrCreate

		import spark.implicits._
		
		// Let user decide features here
		
		println("Reading input file")
		val table = spark.read.parquet(inputFilePath)

		println("Partitioning training data")
		val trainDF = table.filter($"year" =!= YEAR)
			.map(r => MakeLabeledPoint(r, includeFeatures))
			.withColumnRenamed("_1", "county_id")
			.withColumnRenamed("_2", "state")
			.withColumnRenamed("_3", "county")
			.withColumn("label", $"_4".getField("label"))
			.withColumn("features", $"_4".getField("features"))
			.drop("_4")
			.orderBy(rand(SEED))
		trainDF.cache

		println("Partitioning test data")
		val testDF = table.filter($"year" === YEAR)
			.map(r => MakeLabeledPoint(r, includeFeatures))
			.withColumnRenamed("_1", "county_id")
			.withColumnRenamed("_2", "state")
			.withColumnRenamed("_3", "county")
			.withColumn("label", $"_4".getField("label"))
			.withColumn("features", $"_4".getField("features"))
			.drop("_4")

		println("Configuring LR")
		val lr = new LinearRegression()
			.setMaxIter(100)
			.setRegParam(0.3)
			.setElasticNetParam(0.8)

		println("Fitting model to training data")
		val lrModel = lr.fit(trainDF)

		val NAICSCodes = Array("11_Agriculture", "21_Mining", "22_Utilities",
                	"23_Construction", "31_Manufacturing", "42_Wholesale",
                	"44_Retail", "48_Transportation", "51_Information",
                	"52_Finance", "53_Real_Estate", "54_Professional",
                	"55_Management", "56_Administrative", "61_Educational",
                	"62_Health_Care", "71_Entertainment", "72_Accommodation_Food",
                	"81_Other", "99_Unclassified")

	        val BusinessSizes = Array[String]("<5", "5-9", "10-19", "20-49", "50-99", "100-249", "250-499", "500-999", "1,000-1,499", "1,500-2,499", "2,500-4,999", "5,000+")
		var coefTags = Array[String]("% Male", "% Female", "% Hispanic", "% White", "% Black", "% Native", "% Other")
		NAICSCodes.foreach(code => BusinessSizes.foreach(size => coefTags = coefTags ++ Array[String](code + ":" + size)))
		coefTags = coefTags ++ Array("Less than highschool", "Highschool", "Some college or associates", "Bachelors or higher")		

		println("Fitting complete, gathering output...")
		val coefTable = spark.sparkContext.parallelize(lrModel.coefficients.toArray.zip(coefTags.zipWithIndex))
                        .toDF.withColumnRenamed("_1", "coefficients")
			.withColumn("feature", expr("_2._1"))
			.withColumn("index", expr("_2._2"))
			.drop("_2")

		val lrPred = lrModel.transform(testDF).withColumn("goodness", $"prediction" / $"label")

		println("Writing model file to " + outputPath + ".model")
		lrModel.save(outputPath + ".model")

		println("Writing summary to " + outputPath + "summary.parquet")
		spark.sparkContext.parallelize(Array(
			("root mean squared error" , lrModel.summary.rootMeanSquaredError),
			("r2", lrModel.summary.r2),
			("year", YEAR.toDouble),
			("seed", SEED.toDouble)))
			.toDF.write.parquet(outputPath + "summary.parquet")
			

		println("Writing coefficients file to " + outputPath + "coef.parquet")
		coefTable.write.parquet(outputPath + "coef.parquet")
		
		println("Writing predictions file to " + outputPath + "pred.parquet")
                lrPred.write.parquet(outputPath + "pred.parquet")

		spark.stop
	}

        def MakeLabeledPoint(row: Row, includeFeatures: Array[Boolean]) : (Int,String,String,LabeledPoint) = {
                val fips_combined : Int = row.getInt(row.fieldIndex("fips_combined"))
		val state : String = row.getString(row.fieldIndex("state"))
		val county : String = row.getString(row.fieldIndex("county"))
		val avg_agi : Double = row.getDouble(row.fieldIndex("avg_agi_by_pop"))
                val pop : Double = row.getInt(row.fieldIndex("total_population")).toDouble
		val men : Double = row.getInt(row.fieldIndex("men")).toDouble
		val women : Double = row.getInt(row.fieldIndex("women")).toDouble
		val hispanic : Double = row.getDouble(row.fieldIndex("hispanic")) / 100
		val white : Double = row.getDouble(row.fieldIndex("white")) / 100
		val black : Double = row.getDouble(row.fieldIndex("black")) / 100
		val native : Double = row.getDouble(row.fieldIndex("native")) / 100
		val other : Double = 1 - hispanic - white - black - native
		val gender : Array[Double] = Array(men/pop, women/pop)
		val race : Array[Double] = Array(hispanic, white, black, native, other)

		// Business features
                val totals_array : Array[Int] = row.getAs[Seq[Int]](row.fieldIndex("totals")).toArray
                val total_est : Double = totals_array(0).toDouble
                val ests_array : Array[Array[Double]] = row.getAs[Seq[Seq[Int]]](row.fieldIndex("ests"))
                        .toArray.map(_.toArray.map(_.toDouble/total_est))
                        .slice(1,totals_array.length)
                var business = Array[Double]()
                ests_array.foreach(in_ar => business = business ++ in_ar)

		// Education features
		val less_than_hs : Double = row.getInt(row.fieldIndex("less_than_highschool")).toDouble / pop
		val highschool : Double = row.getInt(row.fieldIndex("highschool")).toDouble / pop
		val some_college : Double = row.getInt(row.fieldIndex("some_college")).toDouble / pop
		val bachelors : Double = row.getInt(row.fieldIndex("bachelors")).toDouble / pop
		val education = Array[Double](less_than_hs, highschool, some_college, bachelors)

		// Add features based on user seletion
		var features : Array[Double] = Array[Double]()
		if (includeFeatures(0)) features = features ++ gender
		if (includeFeatures(1)) features = features ++ race
		if (includeFeatures(2)) features = features ++ business
		if (includeFeatures(3)) features = features ++ education

                (fips_combined, state, county, new LabeledPoint(avg_agi, Vectors.dense(features)))
        }

}
