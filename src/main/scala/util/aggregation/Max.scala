package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

class Max extends AggregateFunction[DataPoint[Double], DataPoint[Double], DataPoint[Double]]{
  override def createAccumulator(): DataPoint[Double] = new DataPoint[Double](0L, "", Double.MinValue)

  override def add(in: DataPoint[Double],
                   acc: DataPoint[Double]): DataPoint[Double] = {
    if (in.value > acc.value) {
      new DataPoint[Double](math.max(acc.t, in.t), acc.label, in.value)
    } else {
      new DataPoint[Double](math.max(acc.t, in.t), acc.label, acc.value)
    }
  }

  override def getResult(acc: DataPoint[Double]): DataPoint[Double] = acc

  override def merge(acc: DataPoint[Double],
                     acc1: DataPoint[Double]): DataPoint[Double] = {
    if (acc.value > acc1.value) {
      new DataPoint[Double](math.max(acc.t, acc1.t), acc.label, acc.value)
    } else {
      new DataPoint[Double](math.max(acc.t, acc1.t), acc.label, acc1.value)
    }
  }
}
