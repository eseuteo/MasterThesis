package outlier
import data.DataPoint
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.joda.time.DateTime
import util.aggregation.{Average, Max, Min, Stdev}
import util.interpolation.Interpolation

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object AggregationExperimentMain {
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

    val mimicDataAvg = mimicDataWithTimestamps
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .aggregate(new Average())

    val mimicDataStd = mimicDataWithTimestamps
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .aggregate(new Stdev())

    val mimicDataMin = mimicDataWithTimestamps
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .aggregate(new Min())

    val mimicDataMax = mimicDataWithTimestamps
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .aggregate(new Max())

    val mimicDataAvgStd = mimicDataAvg
      .join(mimicDataStd)
      .where(t => t._1)
      .equalTo(t => t._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply((t1, t2) => (t1._1, t1._3, t2._3))

    val mimicDataAvgStdMin = mimicDataAvgStd
      .join(mimicDataMin)
      .where(t => t._1)
      .equalTo(t => t._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply((t1, t2) => (t1._1, t1._2, t1._3, t2._3))

    val mimicDataAllAggregates = mimicDataAvgStdMin
      .join(mimicDataMax)
      .where(t => t._1)
      .equalTo(t => t._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply((t1, t2) => (t1._1, t1._2, t1._3, t1._4, t2._3))

    mimicDataAllAggregates.print()



    // mimicDataInterpolated.map(_.toString).addSink(kafkaProducer)

//    val sink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(outCsvFile), new SimpleStringEncoder[String]("UTF-8"))
//      .build()
//
//    mimicDataAllAggregates.map(_.toString).addSink(sink)
    env.execute("InterpolationMain")

  }
}
