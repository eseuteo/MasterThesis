package util.interpolation

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.interpolation.InterpolationUtils.getTimestampOffset

class CustomInterpolation(numOfExpectedDataPoints: Int)
  extends ProcessWindowFunction[DataPoint[Double], DataPoint[Double], String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[DataPoint[Double]],
                       out: Collector[DataPoint[Double]]): Unit = {

    val dataPointsInWindow = elements.size
    val timestampOffset = getTimestampOffset(context.window, elements.toList, 10)
    if (dataPointsInWindow < numOfExpectedDataPoints) {
      val expectedDataPointTimestamp: Long = context.window.getStart + timestampOffset + (numOfExpectedDataPoints / 2) * 60000
      val centerElementList = elements.filter(e => e.t == expectedDataPointTimestamp)
      val centerElement = if (centerElementList.size > 0) {
        centerElementList.toList(0)
      } else {
        null
      }

      if (centerElement == null) {
        val nearestNeighborBeforeList = elements.filter(e => e.t < expectedDataPointTimestamp).toList
        val nearestNeighborAfterList = elements.filter(e => e.t > expectedDataPointTimestamp).toList

        val nearestNeighborBefore = if (nearestNeighborBeforeList.size == 0) {
          new DataPoint[Double](0, key, 0.0)
        } else {
          nearestNeighborBeforeList.reverse(0)
        }

        val nearestNeighborAfter = if (nearestNeighborAfterList.size == 0) {
          new DataPoint[Double](0, key, 0.0)
        } else {
          nearestNeighborAfterList(0)
        }
        out.collect(new DataPoint[Double](expectedDataPointTimestamp, key,
          getWeightedMean(nearestNeighborBefore, nearestNeighborAfter, expectedDataPointTimestamp)))
      } else {
        out.collect(centerElement)
      }
    } else {
      out.collect(elements.toList(numOfExpectedDataPoints / 2))
    }

  }

  def getWeightedMean(nnBefore: DataPoint[Double], nnAfter: DataPoint[Double], t: Long): Double = {
    val weightBefore: Double = getWeight(nnBefore.t, t)
    val weightAfter: Double = getWeight(nnAfter.t, t)

    (nnBefore.value * weightBefore + nnAfter.value * weightAfter) / (weightBefore + weightAfter)
  }

  def getWeight(tNeighbor: Long, t: Long): Double = {
    var ans = 0.0
    if (tNeighbor == 0) {
      ans = 0.0
    } else {
      ans = math.pow(2, numOfExpectedDataPoints - Math.abs(tNeighbor - t) / 60000)
    }
    ans
  }

}
