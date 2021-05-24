package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class Max extends AggregateFunction[DataPoint[Double], DataPoint[Double], DataPoint[Double]]{
  override def createAccumulator(): DataPoint[Double] = new DataPoint[Double](0L, "", 0.0)

  override def add(in: DataPoint[Double],
                   acc: DataPoint[Double]): DataPoint[Double] = {
    if (in.value > acc.value) {
      in
    } else {
      acc
    }
  }

  override def getResult(acc: DataPoint[Double]): DataPoint[Double] = acc

  override def merge(acc: DataPoint[Double],
                     acc1: DataPoint[Double]): DataPoint[Double] = {
    if (acc.value > acc1.value) {
      acc
    } else {
      acc1
    }
  }
}
