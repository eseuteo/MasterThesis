package outlier

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class SofaTumblingWindow() extends ProcessWindowFunction[DataPoint[Double], DataPoint[Double], String, TimeWindow]{
  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[DataPoint[Double]]): Unit = {
    out.collect(elements.toList(elements.size-1))
  }
}
