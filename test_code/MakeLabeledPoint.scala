import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint

def MakeLabeledPoint(row: Row) : (String, LabeledPoint) = {
	val agi : Double = row.getDouble(row.fieldIndex("avg_agi"))
	val county_id : String = row.getString(row.fieldIndex("county_id"))
	val pop : Double = row.getString(row.fieldIndex("total_population")).toDouble
	val men : Double = row.getString(row.fieldIndex("men")).toDouble
	val women : Double = row.getString(row.fieldIndex("women")).toDouble
	val hispanic : Double = row.getString(row.fieldIndex("hispanic")).toDouble / 100
	val white : Double = row.getString(row.fieldIndex("white")).toDouble / 100
	val black : Double = row.getString(row.fieldIndex("black")).toDouble / 100
	val native : Double = row.getString(row.fieldIndex("native")).toDouble / 100
	val other : Double = 1 - hispanic - white - black - native
	val demo : Array[Double] = Array(men/pop, women/pop, hispanic, white, black, native, other)

	val totals_array : Array[Int] = row.getAs[Seq[Int]](row.fieldIndex("totals")).toArray
	val total_est : Double = totals_array(0).toDouble
	val ests_array : Array[Array[Double]] = row.getAs[Seq[Seq[Int]]](row.fieldIndex("ests"))
		.toArray.map(_.toArray.map(_.toDouble/total_est))
		.slice(1,totals_array.length)

	var est = Array[Double]()//Array(total_est)

	ests_array.foreach(in_ar => est = est ++ in_ar)

	(county_id, new LabeledPoint(agi, Vectors.dense(demo ++ est)))
}
