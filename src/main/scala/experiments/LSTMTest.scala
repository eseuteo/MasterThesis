package experiments

import models.LSTMSequenceClassifier
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.api.java.tuple.Tuple5
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object LSTMTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val fileName = parameters.getRequired("input")
    val lstmModelDir = parameters.getRequired("modelDir")

    val vitals =
      env.readTextFile(fileName).filter(t => !t.contains("DateVitals"))

    val watermarkStrategy = WatermarkStrategy
      .forMonotonousTimestamps()
      .withTimestampAssigner(
        new SerializableTimestampAssigner[
          Tuple4[String, Long, String, Array[Double]]
        ] {
          override def extractTimestamp(
              t: Tuple4[String, Long, String, Array[Double]],
              l: Long
          ): Long = t.f1
        }
      )

    val vitalsWithTimestamps = vitals
      .map(t => {
        val data = t.split(",")
        new Tuple4[String, Long, String, Array[Double]](
          "patientId",
          getTimestamp(data(1)),
          data(23),
          data.slice(2, 22).map(t => t.toDouble)
        )
      })
      .assignTimestampsAndWatermarks(watermarkStrategy)

    val output: DataStream[Tuple5[String, String, String, Double, Double]] =
      vitalsWithTimestamps
        .keyBy(t => t.f0)
        .window(SlidingEventTimeWindows.of(Time.hours(3), Time.hours(1)))
        .process(new LSTMSequenceClassifier(lstmModelDir))

    output.print()
//        .window(SlidingEventTimeWindows.of(Time.hours(3), Time.hours(1)))
//        .process(new LSTMSequenceClassifier(lstmModelDir))

//    val sequenceClass = vitalsWithTimestamps
//      .keyBy(t => t)
//      .window(SlidingEventTimeWindows.of(Time.hours(3), Time.hours(1)))
//      .trigger(CountTrigger.of(3))
//      .process(new LSTMSequenceClassifier(lstmModelDir))

//    sequenceClass.print()

    env.execute("LSTMTest")
  }

  def getTimestamp(str: String) = {
    LocalDateTime
      .parse(str, DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"))
      .atZone(ZoneId.systemDefault())
      .toInstant
      .toEpochMilli
  }
}
