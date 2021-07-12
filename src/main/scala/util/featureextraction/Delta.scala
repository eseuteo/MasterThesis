package util.featureextraction

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Delta(key: String = "delta") extends ProcessWindowFunction [DataPoint[Double], DataPoint[Double], String, TimeWindow] {
  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[DataPoint[Double]]): Unit = {
    if (elements.toList.size >= 2) {
      val fKminus1 = elements.toList(0)
      val fK = elements.toList(1)
      out.collect(new DataPoint[Double](fK.t, key, fK.value - fKminus1.value))
    }
  }
}
