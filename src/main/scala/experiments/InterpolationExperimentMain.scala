package experiments

import data.DataPoint
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import util.interpolation.Interpolation

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object InterpolationExperimentMain {
  def main(args: Array[String]): Unit = {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val signals = Array("HR", "ABPSys", "ABPDias", "ABPMean", "RESP", "SpO2")

    val mimicFile = parameters.getRequired("input")
    val outCsvFile = parameters.getRequired("output")
    val kafkaTopic = parameters.getRequired("kafkaTopic")

    val orderMA = parameters.getRequired("orderMA").toLong
    val slideMA = parameters.getRequired("slideMA").toLong
    println("  Input file: " + mimicFile)
    println("  Result sink in kafka topic: " + kafkaTopic)

    // Properties for writing stream in Kafka
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val kafkaProducer = new FlinkKafkaProducer[String](kafkaTopic, new SimpleStringSchema(), properties)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val mimicData = env.readTextFile(mimicFile)

    val watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[DataPoint[Double]] {
        override def extractTimestamp(t: DataPoint[Double], l: Long): Long = t.t
      })

    val mimicDataWithTimestamps: DataStream[DataPoint[Double]] = mimicData.flatMap(line => {
      val data: Array[String] = line.split(",")
      val timestamp = LocalDateTime.parse(data(0), DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu")).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
      var list: ListBuffer[DataPoint[Double]] = new ListBuffer[DataPoint[Double]]
      (data.slice(1, data.length), signals).zipped
        .foreach((item, signal) => {
          if (signal.equals("RESP")) {
            list += new DataPoint[Double](timestamp, signal, item.toDouble)}
        })
      list
    }).assignTimestampsAndWatermarks(watermarkStrategy)

    val mimicDataInterpolated = mimicDataWithTimestamps.keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1))).process(new Interpolation(mode = "nocb"))

    mimicDataInterpolated.map(_.toString).addSink(kafkaProducer)

    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outCsvFile), new SimpleStringEncoder[String]("UTF-8"))
      .build()

    mimicDataInterpolated.map(_.toString).addSink(sink)
    env.execute("InterpolationMain")

  }
}
