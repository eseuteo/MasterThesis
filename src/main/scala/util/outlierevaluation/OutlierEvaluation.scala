package util.outlierevaluation

import data.DataPoint
import org.apache.commons.math3.util.FastMath.abs
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class OutlierEvaluation(range: Double, k: Int)
    extends ProcessWindowFunction[DataPoint[Double], DataPoint[
      Double
    ], String, TimeWindow] {
  override def process(
      key: String,
      context: Context,
      elements: Iterable[DataPoint[Double]],
      out: Collector[DataPoint[Double]]
  ): Unit = {
    val inputList = elements.toList
    println(inputList.length)

    val currentDatapoint = inputList(inputList.length / 2)

    val sum: Double = inputList.map(_.value).sum - currentDatapoint.value
    val averageValue = sum / (inputList.length - 1)

    if (abs(averageValue - currentDatapoint.value) < range) {
      out.collect(currentDatapoint)
    }
  }
}
