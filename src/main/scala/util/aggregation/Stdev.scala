package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

// items in tuple: label, sum of x, squared sum of x, count
class Stdev extends AggregateFunction[DataPoint[Double], (String, Double, Double, Long), (String, Double)] {
  override def createAccumulator(): (String, Double, Double, Long) = ("", 0.0, 0.0, 0L)

  override def add(in: DataPoint[Double],
                   acc: (String, Double, Double, Long)): (String, Double, Double, Long) = {
    if (acc._1 == "") {
      (in.label, in.value, in.value * in.value, 1)
    } else {
      (acc._1, acc._2 + in.value, acc._3 + in.value * in.value, acc._4 + 1)
    }
  }

  override def getResult(acc: (String, Double, Double, Long)): (String, Double) = {
    (acc._1, Math.sqrt((acc._3 / acc._4) - (Math.pow(acc._2 / acc._4, 2))))
  }

  override def merge(acc: (String, Double, Double, Long),
                     acc1: (String, Double, Double, Long)): (String, Double, Double, Long) = {
    (acc._1, acc._2 + acc1._2, acc._3 + acc1._3, acc._4 + acc1._4)
  }
}
