import data.DataPoint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import util.aggregation._

class AggregationSpec extends AnyFlatSpec with Matchers {
  "Average" should "Obtain windowed average from a DataStream" in {
    val averageFunction: Average = new Average()

    var acc = averageFunction.createAccumulator()
    acc = averageFunction.add(new DataPoint[Double](1L, "aaa", 1.0), acc)
    acc = averageFunction.add(new DataPoint[Double](2l, "aaa", 2.0), acc)

    averageFunction.getResult(acc) should be (2L, "aaa", 1.5)
    averageFunction.getResult(acc) should not be (1L, "aaa", 1.5)
  }

  "Stdev" should "Obtain windowed standard deviation from a DataPoint[Double] DataStream" in {
    val stdevFunction: Stdev = new Stdev()

    var acc = stdevFunction.createAccumulator()
    acc = stdevFunction.add(new DataPoint[Double](1L, "aaa", 1.0), acc)
    acc = stdevFunction.add(new DataPoint[Double](2l, "aaa", 2.0), acc)

    stdevFunction.getResult(acc) should be (2L, "aaa", 0.5)
    stdevFunction.getResult(acc) should not be (1L, "aaa", 0.5)
  }

  "Min" should "Obtain windowed min from a DataPoint[Double] DataStream" in {
    val minFunction: Min = new Min()

    var acc = minFunction.createAccumulator()
    acc = minFunction.add(new DataPoint[Double](1L, "aaa", 1.0), acc)
    acc = minFunction.add(new DataPoint[Double](2l, "aaa", 2.0), acc)

    minFunction.getResult(acc) should be (2L, "aaa", 1.0)
    minFunction.getResult(acc) should not be (1L, "aaa", 1.0)
  }

  "Max" should "Obtain windowed max from a DataPoint[Double] DataStream" in {
    val maxFunction: Max = new Max()

    var acc = maxFunction.createAccumulator()
    acc = maxFunction.add(new DataPoint[Double](1L, "aaa", 1.0), acc)
    acc = maxFunction.add(new DataPoint[Double](2l, "aaa", 2.0), acc)

    maxFunction.getResult(acc) should be (2L, "aaa", 2.0)
    maxFunction.getResult(acc) should not be (1L, "aaa", 2.0)
  }
}
