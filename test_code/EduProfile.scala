import org.apache.spark.sql.Row

class EduProfile(
	val fips_combined : String
	)
} 

object EduProfileFactory {
	def apply(r: Row) = {
		val ID: String = r.getString(0).substring(9)
		val pop_18_24 : Int = 
	}
}
