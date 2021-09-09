package util.signalgeneration

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple4

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

class GenerateTuplesForModel
    extends MapFunction[String, Tuple4[String, Long, String, Array[Double]]] {
  override def map(t: String): Tuple4[String, Long, String, Array[Double]] = {
    val data = t.split(",")
    val date = LocalDateTime
      .parse(
        data(0).dropRight(4),
        DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
      )
      .atZone(ZoneId.systemDefault())
      .toInstant
      .toEpochMilli
    val input = data.slice(2, 59).map(_.toDouble)
    new Tuple4[String, Long, String, Array[Double]](
      LocalDateTime.parse(data(0).dropRight(4), DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")).getYear.toString,
      date,
      "label",
      input
    )
  }
}
