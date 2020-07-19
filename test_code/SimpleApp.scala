import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    println("Hello, world!")
    spark.stop()
  }
}