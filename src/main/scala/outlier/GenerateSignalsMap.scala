package outlier

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

case class GenerateSignalsMap() extends ProcessWindowFunction[DataPoint[Double], (Long, String), Long, TimeWindow]{
  override def process(key: Long, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[(Long, String)]): Unit = {
    val signalsMap = new mutable.HashMap[String, Double]()
    elements.map(t => {
      val dataPointValue: Double = t.value
      signalsMap.put(t.label, dataPointValue)
    })

    val basicLabels = List("HR", "RESP", "ABPMean", "ABPSys", "ABPDias", "SpO2")
    val correlationLabels = basicLabels.combinations(2).toList.map(t => t.mkString).map(t => s"Corr$t").toList
    val sampEnLabels = basicLabels.map(t => s"Entropy$t")
    val otherLabels = basicLabels.flatMap(t => List(s"stdev$t", s"avg$t", s"min$t", s"max$t")).toList

    val allLabels = correlationLabels ++ sampEnLabels ++ otherLabels ++ List("SOFA_SCORE")

    val date = new Date(elements.toList(0).t)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    var csvRow = s"${sdf.format(date)},"
    for (label <- allLabels) {
      val currentValue = if (signalsMap.contains(label)) signalsMap(label) else ""
      csvRow = csvRow + label + ":" + currentValue + ","
    }
    out.collect((key, csvRow.dropRight(1)))
  }
}
