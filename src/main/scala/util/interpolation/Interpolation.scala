package util.interpolation

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.interpolation.InterpolationUtils.getTimestampOffset
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator
import org.apache.commons.math3.exception.{NumberIsTooSmallException, OutOfRangeException}

class Interpolation(timeMeasure: String = "minutes", slideSizeInMinutes: Int = 10, mode: String = "linear")
  extends ProcessWindowFunction[DataPoint[Double], DataPoint[Double], String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[DataPoint[Double]],
                       out: Collector[DataPoint[Double]]): Unit = {
    val timestampOffset = getTimestampOffset(context.window, elements.toList, slideSizeInMinutes)

    val expectedTimestamp: Long = {
      context.window.getStart - timestampOffset + (context.window.getEnd - context.window.getStart) / 2
    }

    val midDataPoint = elements.toList.filter(p => p.t == expectedTimestamp).headOption

    if (midDataPoint.isEmpty) {
      val previousDataPoint = elements.toList.filter(p => p.t < expectedTimestamp).reverse.headOption
      val nextDataPoint = elements.toList.filter(p => p.t > expectedTimestamp).headOption

      mode match {
        case "linear" => out.collect(new DataPoint[Double](expectedTimestamp, key,
          getLinearInterpolation(previousDataPoint, nextDataPoint, expectedTimestamp)))
        case "locb" => {
          val dataPointValue: Double = if (previousDataPoint.isEmpty) {
            0.0
          } else {
            previousDataPoint.head.value
          }
          out.collect(new DataPoint[Double](expectedTimestamp, key, dataPointValue))
        }
        case "nocb" => {
          val dataPointValue: Double = if (nextDataPoint.isEmpty) {
            0.0
          } else {
            nextDataPoint.head.value
          }
          out.collect(new DataPoint[Double](expectedTimestamp, key, dataPointValue))
        }
        case "spline" => {
          val interpolatedValue: Double = try {
            val interpolator = new SplineInterpolator()
              .interpolate(elements.map(p => p.t.toDouble).toArray, elements.map(p => p.value).toArray)
            interpolator.value(expectedTimestamp.toDouble)
          } catch {
            case e: NumberIsTooSmallException => 0.0
            case e: OutOfRangeException => 0.0
          }
          out.collect(new DataPoint[Double](expectedTimestamp, key, interpolatedValue))
        }
      }
    } else {
      out.collect(midDataPoint.head)
    }
  }

  def getLinearInterpolation(prev: Option[DataPoint[Double]], next: Option[DataPoint[Double]], time: Long): Double = {
    if (!prev.isEmpty && !next.isEmpty) {
      val weight: Double = (time - prev.head.t).toDouble / (next.head.t - prev.head.t)
      prev.head.value * (1 - weight) + next.head.value * weight
    } else if (!prev.isEmpty) {
      prev.head.value
    } else if (!next.isEmpty) {
      next.head.value
    } else {
      0.0
    }
  }
}
