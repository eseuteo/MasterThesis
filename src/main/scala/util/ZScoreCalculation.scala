package util

import data.DataPoint
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

class ZScoreCalculation extends RichMapFunction[DataPoint[Double], DataPoint[Double]] {
  private var zScoreState : ValueState[(Integer, Double, Double)] = _

  override def map(in: DataPoint[Double]): DataPoint[Double] = {
    val tmpZScoreState = zScoreState.value()
    var state = if (tmpZScoreState != null) {
      zScoreState.value()
    } else {
      (1 : Integer , 0.0, 0.0)
    }

    var n : Integer = state._1
    var sumX : Double = state._2 + in.value
    val meanX = sumX / n
    val sumXMinusMeanX = state._3 + pow2(in.value - meanX)
    val stdevX = Math.sqrt(sumXMinusMeanX/n)
    var zScore = 0.0

    if (stdevX > 0) {
      zScore = (in.value - meanX) / stdevX
    } else if (meanX > 0) {
      zScore = in.value - meanX
    }

    n = n + 1
    zScoreState.update((n, sumX, sumXMinusMeanX))

    var dataPoint = new DataPoint[Double](in.t, in.label, in.value)
    dataPoint.zScore = zScore
    dataPoint
  }

  override def open(parameters: Configuration): Unit = {
    zScoreState = getRuntimeContext.getState(
      new ValueStateDescriptor[(Integer, Double, Double)]("Z-Score", createTypeInformation[(Integer, Double, Double)])
    )
  }

  def pow2(v: Double) = v * v
}
