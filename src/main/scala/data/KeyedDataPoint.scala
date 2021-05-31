package data

import java.text.SimpleDateFormat
import java.util.Date

class KeyedDataPoint[T](
    timestampMs: Long = 0,
    label: String = null,
    value: T = null,
    var key1: String = null,
    var isInlier: Boolean = false
) extends DataPoint[T](timestampMs, label: String, value) {

  override def toString = {
    val date = new Date(timestampMs)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    s"${sdf.format(date)},$key,${value}"
  }
}
