package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class Average extends AggregateFunction[DataPoint[Double], (String, Double, Int), (String, Double)] {
  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

  override def add(in: DataPoint[Double],
                   acc: (String, Double, Int)): (String, Double, Int) = {
    if (acc._1 == "") {
      (in.label, in.value, 1)
    } else {
      (acc._1, acc._2 + in.value, acc._3 + 1)
    }
  }

  override def getResult(acc: (String, Double, Int)): (String, Double) = (acc._1, acc._2 / acc._3)

  override def merge(acc: (String, Double, Int),
                     acc1: (String, Double, Int)): (String, Double, Int) = (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)
}
