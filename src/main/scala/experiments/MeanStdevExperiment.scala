package experiments

import data.DataPoint
import data.DataPoint
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import util.aggregation.{Average, Max, Min, Stdev}
import util.featureextraction.{Correlation, Delta, MultiScaleEntropy, SampleEntropy}
import util.interpolation.Interpolation
import util.normalization.ZScoreCalculation
import util.signalgeneration.GenerateSignalsMap

import java.time.{Duration, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object MeanStdevExperiment {
  def getMean(
               signal: DataStream[DataPoint[Double]],
               orderMA: Long,
               slideMA: Long
             ) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(1)))
      .aggregate(new Average())
  }

  def getStdev(
                signal: DataStream[DataPoint[Double]],
                orderMA: Long,
                slideMA: Long
              ) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(1)))
      .aggregate(new Stdev())
  }

  def main(args: Array[String]): Unit = {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val signals = Array(
      "HR",
      "ABPSys",
      "ABPDias",
      "ABPMean",
      "RESP",
      "SpO2",
      "SOFA_SCORE"
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

    val sinkOriginal: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path("HRoriginal"),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .build()

    val sinkInterpolation: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path("HRinterpolation"),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .build()

    val sinkMean: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path("HRMean"),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .build()

    val sinkStdev: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path("HRStdev"),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .build()

    val conf: Configuration = new Configuration()

    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val mimicData =
      env.readTextFile(mimicFile).filter(t => !t.contains("TIME"))

    val watermarkStrategy = WatermarkStrategy
      .forMonotonousTimestamps()
      .withTimestampAssigner(
        new SerializableTimestampAssigner[DataPoint[Double]] {
          override def extractTimestamp(t: DataPoint[Double], l: Long): Long =
            t.t
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

    mimicDataWithTimestamps.filter(t => t.label == "ABPMean").map(t => t.toString).addSink(sinkOriginal)

    val hrProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPMean", 60, 1)

    hrProcessedSignal.map(t => t.toString).addSink(sinkInterpolation)

    getMean(hrProcessedSignal, 10, slideMA).map(t => t.toString).addSink(sinkMean)
    getStdev(hrProcessedSignal, orderMA, slideMA).map(t => t.toString).addSink(sinkStdev)

    env.execute("MimicDataJob")

  }

  def getDelta(signal: DataStream[DataPoint[Double]], label: String): DataStream[DataPoint[Double]] = {
    signal.keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.minutes(1)))
      .process(new Delta())
      .map(t => new DataPoint[Double](t.t, s"delta$label", t.value))
  }

  def getJoin(
               signalA: DataStream[DataPoint[Double]],
               signalB: DataStream[DataPoint[Double]]
             ) = {
    signalA.join(signalB).where(t => t.t).equalTo(t => t.t)
  }

  def getSampleEntropy(
                        signal: DataStream[DataPoint[Double]],
                        label: String,
                        orderMA: Long
                      ) = {
    signal
      .keyBy(t => t.label)
      .window(TumblingEventTimeWindows.of(Time.minutes(orderMA)))
      .process(new MultiScaleEntropy(0.01, 1, 2))
      .map(t => new DataPoint[Double](t._1, s"Entropy$label", t._4))
  }

  def getCorrelation(
                      signalA: DataStream[DataPoint[Double]],
                      signalB: DataStream[DataPoint[Double]],
                      labelA: String,
                      labelB: String,
                      windowSize: Long,
                      windowSlide: Long
                    ) = {
    signalA
      .union(signalB)
      .map(t => {
        var dataPoint = new DataPoint[Double](t.t, t.label, t.value)
        dataPoint.key = "1"
        dataPoint
      })
      .keyBy(t => t.key)
      .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
      .process(
        new Correlation(labelA, labelB, windowSizeInMinutes = windowSize)
      )
  }

  def processSignal(
                     signal: DataStream[DataPoint[Double]],
                     label: String,
                     windowSize: Long,
                     windowSlide: Long
                   ): DataStream[DataPoint[Double]] = {
    signal
      .filter(t => t.value != -1.0)
      .filter(t => t.label == label)
      .keyBy(t => t.label)
      .window(
        SlidingEventTimeWindows
          .of(Time.minutes(windowSize), Time.minutes(windowSlide))
      )
      .process(
        new Interpolation(mode = "linear", windowSizeInMinutes = windowSize)
      )
      .filter(t=>t.value >50 && t.value <150)
//      .keyBy(t => t.label)
//      .map(new ZScoreCalculation())
//      .filter(t => t.zScore > -3 && t.zScore < 3)
//      .map(t => new DataPoint[Double](t.t, t.label, t.zScore))
  }

}
