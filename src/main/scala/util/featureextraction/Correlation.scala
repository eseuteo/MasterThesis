package util.featureextraction

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Correlation(signalA: String, signalB: String) extends ProcessWindowFunction[DataPoint[Double], Double, String, TimeWindow] {

  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[Double]): Unit = {
    val listX = elements.filter(elem => elem.label.equals(signalA))
    val listY = elements.filter(elem => elem.label.equals(signalB))

    var sx: Double = 0.0
    var sy: Double = 0.0
    var sxx: Double = 0.0
    var syy: Double = 0.0
    var sxy: Double = 0.0

    for ((x, y) <- (listX zip listY)) {
      sx += x.value
      sy += y.value
      sxx += x.value * x.value
      syy += y.value * y.value
      sxy += x.value * y.value
    }

    val n = listX.size
    val cov: Double = sxy / n - sx * sy / n / n
    val sigmax: Double = math.sqrt(sxx / n - sx * sx / n / n)
    val sigmay: Double = math.sqrt(syy / n - sy * sy / n / n)

    out.collect(cov / sigmax / sigmay)
  }
}
