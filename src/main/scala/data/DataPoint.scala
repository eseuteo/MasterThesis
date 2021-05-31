package data

import java.text.SimpleDateFormat
import java.util.Date

class DataPoint[T](val t: Long = 0, val label: String = null, val value: T = null) extends Serializable {
  var outlierVotes = 0
  var zScore = 0.0
  var key = ""

  override def toString = {
    val date = new Date(t)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    s"${sdf.format(date)},$label,$value"
  }
}
