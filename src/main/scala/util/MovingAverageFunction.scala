package util

import data.KeyedDataPoint
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MovingAverageFunction extends ProcessWindowFunction[KeyedDataPoint[Double], KeyedDataPoint[Double], String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[KeyedDataPoint[Double]], out: Collector[KeyedDataPoint[Double]]): Unit = {
      var count = 0
      var winsum = 0.0

      for (in <- elements) {
        winsum = winsum + in.value
        count += 1
      }

      var avgWinSum = 0.0
      if (count > 0) avgWinSum = winsum / (1.0 * count)
      val endWin = context.window.getEnd
      out.collect(new KeyedDataPoint[Double](endWin, key, avgWinSum))
    }
}
