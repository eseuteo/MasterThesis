package outlier

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class GenerateSignalsAggregate extends AggregateFunction[DataPoint[Double], (Long, Array[Double]), (Long, String)]{
  val basicLabels = List("HR", "RESP", "ABPMean", "ABPSys", "ABPDias", "SpO2")
  val correlationLabels = basicLabels.combinations(2).toList.map(t => t.mkString).map(t => s"Corr$t").toList
  val sampEnLabels = basicLabels.map(t => s"Entropy$t")
  val otherLabels = basicLabels.flatMap(t => List(s"stdev$t", s"mean$t", s"min$t", s"max$t")).toList

  val allLabels = basicLabels ++ correlationLabels ++ sampEnLabels ++ otherLabels ++ List("SOFA_SCORE")

  val normalizedLabels = List("HR", "RESP", "ABPMean", "ABPSys", "ABPDias", "SpO2", "SOFA_SCORE")

  override def createAccumulator(): (Long, Array[Double]) = (0L, new Array(allLabels.length))

  override def add(in: DataPoint[Double],
                   acc: (Long, Array[Double])): (Long, Array[Double]) = {
    val index = allLabels.indexOf(in.label)
    val newValue = if (normalizedLabels.contains(in.label)) in.zScore else in.value
    acc._2(index) = newValue
    if (acc._1 == 0L) {
      (in.t, acc._2)
    } else {
      (acc._1, acc._2)
    }
  }

  override def getResult(acc: (Long, Array[Double])): (Long, String) = {
    (acc._1, acc._2.mkString(","))
  }

  override def merge(acc: (Long, Array[Double]),
                     acc1: (Long, Array[Double])): (Long, Array[Double]) = {
    val array = acc._2
    for (i <- 0 to acc1._2.length) {
      if (array(i) == null) {
        array(i) = acc1._2(i)
      }
    }
    (acc._1, array)
  }
}
