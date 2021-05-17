package outlier

import data.KeyedDataPoint
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, TimestampAssigner}
import org.apache.flink.streaming.api.watermark
import org.apache.flink.streaming.api.watermark.Watermark

class MimicTimestamp extends AssignerWithPeriodicWatermarks[KeyedDataPoint[Double]] with Serializable {
  val maxOutOfOrderness = 1000l

  override def getCurrentWatermark: Watermark = new Watermark(System.currentTimeMillis() - maxOutOfOrderness)

  override def extractTimestamp(t: KeyedDataPoint[Double], l: Long): Long = t.t
}
