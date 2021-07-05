package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class Min extends AggregateFunction[DataPoint[Double], (Long, String, Double), DataPoint[Double]]{
  override def createAccumulator(): (Long, String, Double) = (0L, "", Double.MaxValue)

  override def add(in: DataPoint[Double],
                   acc: (Long, String, Double)): (Long, String, Double) = {
    if (in.value < acc._3) {
      (math.max(acc._1, in.t), in.label, in.value)
    } else {
      (math.max(acc._1, in.t), in.label, acc._3)
    }
  }

  override def getResult(acc: (Long, String, Double)): DataPoint[Double] = new DataPoint[Double](acc._1, s"min${acc._2}", acc._3)

  override def merge(acc: (Long, String, Double),
                     acc1: (Long, String, Double)): (Long, String, Double) = {
    if (acc._3 < acc1._3) {
      (math.max(acc._1, acc1._1), acc._2, acc._3)
    } else {
      (math.max(acc._1, acc1._1), acc._2, acc1._3)
    }
  }
}
