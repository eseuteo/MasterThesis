package util.featureextraction

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class SampleEntropy(val m: Int = 2, val r: Double = 0.002) extends ProcessWindowFunction [DataPoint[Double], DataPoint[Double], String, TimeWindow]{
  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[DataPoint[Double]]): Unit = {
    val N = elements.size

    val xmi: IndexedSeq[List[Double]] = for (i <- 0 to N - m)
      yield elements.map(t => t.value).toList.slice(from=i, until=i+m)

    val xmiCombinations = xmi.combinations(2)

    val B = xmiCombinations
      .map(t => chebyshevDistance(t.head, t.tail.head))
      .map(t => if (t <= r) 1 else 0)
      .sum

    val xm: IndexedSeq[List[Double]] = for (i <- 0 to N - (m + 1))
      yield elements.map(t => t.value).toList.slice(from=i, until=i+m+1)

    val xmCombinations = xm.combinations(2)

    val A = xmCombinations
      .map(t => chebyshevDistance(t.head, t.tail.head))
      .map(t => if (t <= r) 1 else 0)
      .sum

    if (B != 0) {
      out.collect(new DataPoint[Double](elements.toList(elements.size/2).t, f"SampEn${elements.toList(0).label}", -math.log(A/B)))
    } else {
      out.collect(new DataPoint[Double](elements.toList(elements.size/2).t, f"SampEn${elements.toList(0).label}", 0.0))
    }

  }

  def chebyshevDistance(a: List[Double], b: List[Double]) = {
    (0 until a.size).map(i => math.abs(a(i) - b(i))).max
  }
}
