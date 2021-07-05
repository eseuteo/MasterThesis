package util.interpolation

import data.DataPoint
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.immutable.NumericRange

object InterpolationUtils {

  def getTimestampOffset(l: TimeWindow, list: List[DataPoint[Double]], windowSizeInMinutes: Long): Long = {
    val timestampsExpected: NumericRange[Long] = l.getStart until l.getEnd by (l.getEnd - l.getStart) / windowSizeInMinutes
    val timestampsExpectedLong = timestampsExpected.toList

    getMinDifference(timestampsExpectedLong, list.map(x => x.t), (l.getEnd - l.getStart) / windowSizeInMinutes)
  }

  def getMinDifference(listA: List[Long], listB: List[Long], windowSize: Long): Long = {
    if (Math.abs(listA.head - listB.head) > windowSize) {
      getMinDifference(listA.tail, listB, windowSize)
    } else {
      listA.head - listB.head
    }
  }

}
