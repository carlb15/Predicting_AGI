import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Row

object ModelTrainer {
	def main(args: Array[String]) {
		if (args.length < 3) {
			System.err.println("Usage: ModelTrainer <input_file_path> <coef_file_path> <pred_file_path>")
			System.exit(1)
		}

		val inputFilePath = args(0)
		val coefFilePath = args(1)
		val predFilePath = args(2)

		val spark = SparkSession.builder
			.appName("ModelTrainer")
			.getOrCreate

		import spark.implicits._
		
		val table = spark.read.parquet(inputFilePath)

		val trainDF = table.filter($"year" =!= 2017)
			.map(r => MakeLabeledPoint(r))
			.withColumnRenamed("_1", "county_id")
			.withColumn("label", $"_2".getField("label"))
			.withColumn("features", $"_2".getField("features"))
			.drop("_2")
		trainDF.cache

		val testDF = table.filter($"year" === 2017)
			.map(r => MakeLabeledPoint(r))
			.withColumnRenamed("_1", "county_id")
			.withColumn("label", $"_2".getField("label"))
			.withColumn("features", $"_2".getField("features"))
			.drop("_2")

		val lr = new LinearRegression()
			.setMaxIter(100)
			.setRegParam(0.3)
			.setElasticNetParam(0.8)

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

		val coefTable = spark.sparkContext.parallelize(lrModel.coefficients.toArray.zip(coefTags))
                        .toDF.withColumnRenamed("_1", "coefficients")
			.withColumnRenamed("_2", "feature")

		val lrPred = lrModel.transform(testDF).withColumn("goodness", $"prediction" / $"label")

		coefTable.write.parquet(coefFilePath)
                lrPred.write.parquet(predFilePath)

		spark.stop
	}

        def MakeLabeledPoint(row: Row) : (Int,LabeledPoint) = {
                val fips_combined : Int = row.getInt(row.fieldIndex("fips_combined"))
    		//print(10)
		val avg_agi : Double = row.getDouble(row.fieldIndex("avg_agi_by_pop"))
		//print(9)
                val pop : Double = row.getInt(row.fieldIndex("total_population")).toDouble
                //print(8)
		val men : Double = row.getInt(row.fieldIndex("men")).toDouble
                //print(7)
		val women : Double = row.getInt(row.fieldIndex("women")).toDouble
               	//print(6)
		val hispanic : Double = row.getDouble(row.fieldIndex("hispanic")) / 100
                //print(5)
		val white : Double = row.getDouble(row.fieldIndex("white")) / 100
                //print(4)
		val black : Double = row.getDouble(row.fieldIndex("black")) / 100
                //print(3)
		val native : Double = row.getDouble(row.fieldIndex("native")) / 100
                //print(2)
		val other : Double = 1 - hispanic - white - black - native
                //print(1)
		val demo : Array[Double] = Array(men/pop, women/pop, hispanic, white, black, native, other)

                val totals_array : Array[Int] = row.getAs[Seq[Int]](row.fieldIndex("totals")).toArray
                val total_est : Double = totals_array(0).toDouble
                val ests_array : Array[Array[Double]] = row.getAs[Seq[Seq[Int]]](row.fieldIndex("ests"))
                        .toArray.map(_.toArray.map(_.toDouble/total_est))
                        .slice(1,totals_array.length)

                var est = Array[Double]()

                ests_array.foreach(in_ar => est = est ++ in_ar)

                (fips_combined, new LabeledPoint(avg_agi, Vectors.dense(demo ++ est)))
        }

}
