class TestData(
	val features : Array[Double],
	val target : Int
	)

object RandomTestDataFactory {
	def apply(num : Int) : List[TestData] = {
		var ret = Array.ofDim[TestData](num)
		val r = scala.util.Random
		for (i <- 0 until ret.length) {
			ret(i) = new TestData(
				Array(r.nextDouble, r.nextDouble, r.nextDouble),
				r.nextInt(101)
				)
		}
		ret.toList
	}
}
