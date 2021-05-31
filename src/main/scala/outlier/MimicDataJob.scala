package outlier

import data.{DataPoint, KeyedDataPoint}
import dfki.util.UserDefinedFunctions.ZscoreNormalization
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import util.aggregation.Stdev
import util.featureextraction.Correlation
import util.interpolation.{CustomInterpolation, Interpolation}
import util.{MovingAverageFunction, OutlierEvaluation, ZScoreCalculation, myKeyedProcessFunction}

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object MimicDataJob {
  def main(args: Array[String]): Unit = {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val signals = Array(
//      "Time and date",
      "HR",
      "ABPSys",
      "ABPDias",
      "ABPMean",
      "RESP",
      "SpO2"
    )

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

    val kafkaProducer = new FlinkKafkaProducer[String](
      kafkaTopic,
      new SimpleStringSchema(),
      properties
    )

    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val mimicData = env.readTextFile(mimicFile)

    val watermarkStrategy = WatermarkStrategy
      .forMonotonousTimestamps()
      .withTimestampAssigner(
        new SerializableTimestampAssigner[DataPoint[Double]] {
          override def extractTimestamp(t: DataPoint[Double], l: Long): Long = t.t
        }
      )

    val mimicDataWithTimestamps: DataStream[DataPoint[Double]] = mimicData
      .flatMap(line => {
        val data: Array[String] = line.split(",")
        val timestamp = LocalDateTime
          .parse(data(0), DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
          .atZone(ZoneId.systemDefault())
          .toInstant
          .toEpochMilli
        var list: ListBuffer[DataPoint[Double]] =
          new ListBuffer[DataPoint[Double]]
        (data.slice(1, data.length), signals).zipped.foreach((item, signal) =>
          list += new DataPoint[Double](timestamp, signal, item.toDouble)
        )
        list
      })
      .assignTimestampsAndWatermarks(watermarkStrategy)

    val mimicDataWithoutOutliers = mimicDataWithTimestamps
      .filter(t => t.label == "ABPMean")
      .keyBy(t => {
        t.label
      }).map(new ZScoreCalculation()).filter(p => p.zScore < 2 && p.zScore > -2)

//      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
//      .process(new OutlierEvaluation(Time.minutes(10).toMilliseconds.toInt, 5, 3))

    val mimicDataInterpolated = mimicDataWithoutOutliers
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .process(new Interpolation(mode="linear"))

    val abp = mimicDataWithTimestamps.filter(t => t.label == "ABPMean")
    val hr = mimicDataWithTimestamps.filter(t => t.label == "HR")

    val abpWithKey: DataStream[DataPoint[Double]] = abp.map(t => {
      var datapoint = new DataPoint[Double](t.t, t.label, t.value)
      datapoint.key = "1"
      datapoint
    })
    val hrWithKey: DataStream[DataPoint[Double]] = hr.map(t => {
      var datapoint = new DataPoint[Double](t.t, t.label, t.value)
      datapoint.key = "1"
      datapoint
    })

    val correlation = mimicDataWithTimestamps.map(t => {
      var dataPoint = new DataPoint[Double](t.t, t.label, t.value)
      dataPoint.key = "1"
      dataPoint
    }).keyBy(t => t.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(1000), Time.minutes(1)))
      .process(new Correlation("HR", "HR"))

    correlation.print()

//    val stdev = mimicDataWithoutOutliers
//      .keyBy(t => t.label)
//      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
//      .aggregate(new Stdev())

//    stdev.print()

//    mimicDataInterpolated.print()

//    mimicDataInterpolated.map(_.toString).addSink(kafkaProducer)

    env.execute("MimicDataJob")

  }

}
