package util.aggregation

import data.DataPoint
import org.apache.flink.api.common.functions.AggregateFunction

// items in tuple: label, sum of x, squared sum of x, count
class Stdev
    extends AggregateFunction[DataPoint[
      Double
    ], (Long, String, Double, Double, Long), DataPoint[Double]] {
  override def createAccumulator(): (Long, String, Double, Double, Long) =
    (0L, "", 0.0, 0.0, 0L)

  override def add(
      in: DataPoint[Double],
      acc: (Long, String, Double, Double, Long)
  ): (Long, String, Double, Double, Long) = {
    if (acc._1 == "") {
      (in.t, in.label, in.value, in.value * in.value, 1)
    } else {
      (
        math.max(in.t, acc._1),
        in.label,
        acc._3 + in.value,
        acc._4 + in.value * in.value,
        acc._5 + 1
      )
    }
  }

  override def merge(
      acc: (Long, String, Double, Double, Long),
      acc1: (Long, String, Double, Double, Long)
  ): (Long, String, Double, Double, Long) = {
    (
      math.max(acc._1, acc1._1),
      acc._2,
      acc._3 + acc1._3,
      acc._4 + acc1._4,
      acc._5 + acc1._5
    )
  }

  override def getResult(
      acc: (Long, String, Double, Double, Long)
  ): DataPoint[Double] = {
    new DataPoint[Double](
      acc._1,
      s"stdev${acc._2}",
      Math.sqrt((acc._4 / acc._5) - (Math.pow(acc._3 / acc._5, 2)))
    )
  }
}
