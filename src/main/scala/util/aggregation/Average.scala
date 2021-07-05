package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class Average extends AggregateFunction[DataPoint[Double], (Long, String, Double, Int), DataPoint[Double]] {
  override def createAccumulator(): (Long, String, Double, Int) = (0L, "", 0.0, 0)

  override def add(in: DataPoint[Double],
                   acc: (Long, String, Double, Int)): (Long, String, Double, Int) = {
    if (acc._2 == "") {
      (in.t, in.label, in.value, 1)
    } else {
      (math.max(in.t, acc._1), in.label, acc._3 + in.value, acc._4 + 1)
    }
  }

  override def getResult(acc: (Long, String, Double, Int)): DataPoint[Double] = new DataPoint[Double](acc._1, s"mean${acc._2}", acc._3 / acc._4)

  override def merge(acc: (Long, String, Double, Int),
                     acc1: (Long, String, Double, Int)): (Long, String, Double, Int) = (math.max(acc._1, acc1._1), acc._2, acc._3 + acc1._3, acc._4 + acc1._4)
}
