import org.apache.spark.sql.Row

class BusinessProfile(
	val fipstate: String,
	val fipscty: String,
	val year: Int,
	val nPres: Array[Boolean],
	val totals : Array[Int],
	val ests : Array[Array[Int]]) {

	org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

	def merge(bp : BusinessProfile) : BusinessProfile = {
		if (fipstate != bp.fipstate ||
			fipscty != bp.fipscty ||
			year != bp.year)
			throw new Exception("BusinessProfile mismatch")

		for ((b,i) <- bp.nPres.zipWithIndex) {
			if (b) {
				nPres(i) = true
				totals(i) = bp.totals(i)
				for ((v,j) <- bp.ests(i).zipWithIndex) { ests(i)(j) = v }
			}
		}
		this
	}
} 

object BusinessProfileFactory {
	val ST : Int = 0
	val CTY : Int = 1
	val NAC : Int = 2
	val EST : Int = 3
	val N_START : Int = 4
	val N_END : Int = 15
	val YR : Int = 16
	def apply(r: Row, listOfNAICS: List[String]) = {
		val totals = Array.ofDim[Int](listOfNAICS.size)
		val offset = listOfNAICS.indexOf(r.getString(NAC))
		val nPres = Array.ofDim[Boolean](listOfNAICS.size)
		nPres(offset) = true
		totals(offset) = r.getInt(EST)
		val ests = Array.ofDim[Int](listOfNAICS.size, N_END - N_START + 1)
		for (i <- 0 until N_END-N_START) { ests(offset)(i) = r.getInt(N_START + i) }
		new BusinessProfile(r.getString(ST), 
			r.getString(CTY),
			r.getInt(YR),
			nPres,
			totals,
			ests)
	}
}
