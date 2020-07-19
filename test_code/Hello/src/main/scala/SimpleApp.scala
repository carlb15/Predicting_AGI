import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    val df = spark.read.parquet("ex.parquet")
    df.write.parquet("ex2.parquet")
    spark.stop()
  }
}
