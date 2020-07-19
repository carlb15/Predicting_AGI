import org.apache.spark.sql.Row

class IrsProfile(
	val fipstate: Int,
	val state: String,
	val fipscty: Int,
	val county: String,
	var n1: Int,
	var agi: Int,
	val year: String,
	val county_id: String
	) {

	org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

	def merge(ip : IrsProfile) : IrsProfile = {
		if (fipstate != ip.fipstate ||
			fipscty != ip.fipscty ||
			year != ip.year)
			throw new Exception("IrsProfile mismatch")

		n1 += ip.n1
		agi += ip.agi
		this
	}
} 

object IrsProfileFactory {
	def apply(r: Row) = {
		new IrsProfile(
			r.getInt(r.fieldIndex("STATEFIPS")),
			r.getString(r.fieldIndex("STATE")),
			r.getInt(r.fieldIndex("COUNTYFIPS")),
			r.getString(r.fieldIndex("COUNTYNAME")),
			r.getInt(r.fieldIndex("N1")),
			r.getInt(r.fieldIndex("A00100")),
			r.getString(r.fieldIndex("year")),
			r.getString(r.fieldIndex("county_id"))
			)
	}
}
