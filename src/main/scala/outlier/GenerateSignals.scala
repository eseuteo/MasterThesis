package outlier

import data.DataPoint
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import util.ZScoreCalculation
import util.aggregation.{Average, Max, Min, Stdev}
import util.featureextraction.{Correlation, MultiScaleEntropy}
import util.interpolation.Interpolation

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object GenerateSignals {

  def getMean(signal: DataStream[DataPoint[Double]], orderMA: Long, slideMA: Long) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
      .aggregate(new Average())
  }

  def getStdev(signal: DataStream[DataPoint[Double]], orderMA: Long, slideMA: Long) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
      .aggregate(new Stdev())
  }

  def getMin(signal: DataStream[DataPoint[Double]], orderMA: Long, slideMA: Long) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
      .aggregate(new Min())
  }

  def getMax(signal: DataStream[DataPoint[Double]], orderMA: Long, slideMA: Long) = {
    signal
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
      .aggregate(new Max())
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
      .flatMap((line: String) => {
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

    val hrProcessedSignal = processSignal(mimicDataWithTimestamps, "HR", orderMA, slideMA)
    val respProcessedSignal = processSignal(mimicDataWithTimestamps, "RESP", orderMA, slideMA)
    val abpmProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPMean", orderMA, slideMA)
    val abpsProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPSys", orderMA, slideMA)
    val abpdProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPDias", orderMA, slideMA)
    val spo2ProcessedSignal = processSignal(mimicDataWithTimestamps, "SpO2", orderMA, slideMA)
    val sofascore = processSignal(mimicDataWithTimestamps, "SOFA_SCORE", orderMA, slideMA)

    val hrRespCorrelation = getCorrelation(hrProcessedSignal, respProcessedSignal, "HR", "RESP", orderMA, slideMA)
    val hrAbpmeanCorrelation = getCorrelation(hrProcessedSignal, abpmProcessedSignal, "HR", "ABPMean", orderMA, slideMA)
    val hrAbpsysCorrelation = getCorrelation(hrProcessedSignal, abpsProcessedSignal, "HR", "ABPSys", orderMA, slideMA)
    val hrAbpdiasCorrelation = getCorrelation(hrProcessedSignal, abpdProcessedSignal, "HR", "ABPDias", orderMA, slideMA)
    val hrSpo2Correlation = getCorrelation(hrProcessedSignal, spo2ProcessedSignal, "HR", "SpO2", orderMA, slideMA)
    val respAbpmeanCorrelation = getCorrelation(respProcessedSignal, abpmProcessedSignal, "RESP", "ABPMean", orderMA, slideMA)
    val respAbpsysCorrelation = getCorrelation(respProcessedSignal, abpsProcessedSignal, "RESP", "ABPSys", orderMA, slideMA)
    val respAbpdiasCorrelation = getCorrelation(respProcessedSignal, abpdProcessedSignal, "RESP", "ABPDias", orderMA, slideMA)
    val respSpo2Correlation = getCorrelation(respProcessedSignal, spo2ProcessedSignal, "RESP", "SpO2", orderMA, slideMA)
    val abpmeanAbpsysCorrelation = getCorrelation(abpmProcessedSignal, abpsProcessedSignal, "ABPMean", "ABPSys", orderMA, slideMA)
    val abpmeanAbpdiasCorrelation = getCorrelation(abpmProcessedSignal, abpdProcessedSignal, "ABPMean", "ABPDias", orderMA, slideMA)
    val abpmeanSpo2Correlation = getCorrelation(abpmProcessedSignal, spo2ProcessedSignal, "ABPMean", "SpO2", orderMA, slideMA)
    val abpsysAbpdiasCorrelation = getCorrelation(abpsProcessedSignal, abpdProcessedSignal, "ABPSys", "ABPDias", orderMA, slideMA)
    val abpsysSpo2Correlation = getCorrelation(abpsProcessedSignal, spo2ProcessedSignal, "ABPSys", "SpO2", orderMA, slideMA)
    val abpdiasSpo2Correlation = getCorrelation(abpdProcessedSignal, spo2ProcessedSignal, "ABPDias", "SpO2", orderMA, slideMA)

    val mseHR = getMultiScaleEntropy(hrProcessedSignal, "HR")
    val mseRESP = getMultiScaleEntropy(respProcessedSignal, "RESP")
    val mseABPMean = getMultiScaleEntropy(abpmProcessedSignal, "ABPMean")
    val mseABPSys = getMultiScaleEntropy(abpsProcessedSignal, "ABPSys")
    val mseABPDias = getMultiScaleEntropy(abpdProcessedSignal, "ABPDias")
    val mseSpO2 = getMultiScaleEntropy(spo2ProcessedSignal, "SpO2")

    val meanHR = getMean(hrProcessedSignal, orderMA, slideMA)
    val stdevHR = getStdev(hrProcessedSignal, orderMA, slideMA)
    val minHR = getMin(hrProcessedSignal, orderMA, slideMA)
    val maxHR = getMax(hrProcessedSignal, orderMA, slideMA)

    val meanRESP = getMean(respProcessedSignal, orderMA, slideMA)
    val stdevRESP = getStdev(respProcessedSignal, orderMA, slideMA)
    val minRESP = getMin(respProcessedSignal, orderMA, slideMA)
    val maxRESP = getMax(respProcessedSignal, orderMA, slideMA)

    val meanABPMean = getMean(abpmProcessedSignal, orderMA, slideMA)
    val stdevABPMean = getStdev(abpmProcessedSignal, orderMA, slideMA)
    val minABPMean = getMin(abpmProcessedSignal, orderMA, slideMA)
    val maxABPMean = getMax(abpmProcessedSignal, orderMA, slideMA)

    val meanABPSys = getMean(abpsProcessedSignal, orderMA, slideMA)
    val stdevABPSys = getStdev(abpsProcessedSignal, orderMA, slideMA)
    val minABPSys = getMin(abpsProcessedSignal, orderMA, slideMA)
    val maxABPSys = getMax(abpsProcessedSignal, orderMA, slideMA)

    val meanABPDias = getMean(abpdProcessedSignal, orderMA, slideMA)
    val stdevABPDias = getStdev(abpdProcessedSignal, orderMA, slideMA)
    val minABPDias = getMin(abpdProcessedSignal, orderMA, slideMA)
    val maxABPDias = getMax(abpdProcessedSignal, orderMA, slideMA)

    val meanSpO2 = getMean(spo2ProcessedSignal, orderMA, slideMA)
    val stdevSpO2 = getStdev(spo2ProcessedSignal, orderMA, slideMA)
    val minSpO2 = getMin(spo2ProcessedSignal, orderMA, slideMA)
    val maxSpO2 = getMax(spo2ProcessedSignal, orderMA, slideMA)

    hrProcessedSignal
      .union(respProcessedSignal)
      .union(abpmProcessedSignal)
      .union(abpsProcessedSignal)
      .union(abpdProcessedSignal)
      .union(spo2ProcessedSignal)
      .union(hrRespCorrelation)
      .union(hrAbpmeanCorrelation)
      .union(hrAbpsysCorrelation)
      .union(hrAbpdiasCorrelation)
      .union(hrSpo2Correlation)
      .union(respAbpmeanCorrelation)
      .union(respAbpsysCorrelation)
      .union(respAbpdiasCorrelation)
      .union(respSpo2Correlation)
      .union(abpmeanAbpsysCorrelation)
      .union(abpmeanAbpdiasCorrelation)
      .union(abpmeanSpo2Correlation)
      .union(abpsysAbpdiasCorrelation)
      .union(abpsysSpo2Correlation)
      .union(abpdiasSpo2Correlation)
      .union(meanHR)
      .union(stdevHR)
      .union(minHR)
      .union(maxHR)
      .union(meanRESP)
      .union(stdevRESP)
      .union(minRESP)
      .union(maxRESP)
      .union(meanABPMean)
      .union(stdevABPMean)
      .union(minABPMean)
      .union(maxABPMean)
      .union(meanABPSys)
      .union(stdevABPSys)
      .union(minABPSys)
      .union(maxABPSys)
      .union(meanABPDias)
      .union(stdevABPDias)
      .union(minABPDias)
      .union(maxABPDias)
      .union(meanSpO2)
      .union(stdevSpO2)
      .union(minSpO2)
      .union(maxSpO2)



    env.execute("MimicDataJob")

  }

  def getJoin(signalA: DataStream[DataPoint[Double]], signalB: DataStream[DataPoint[Double]]) = {
    signalA.join(signalB).where(t => t.t).equalTo(t => t.t)
  }

  def getMultiScaleEntropy(signal: DataStream[DataPoint[Double]], label: String) = {
    signal
      .keyBy(t => t.label)
      .window(TumblingEventTimeWindows.of(Time.minutes(100)))
      .process(new MultiScaleEntropy(0.1, 5, 5))
  }

  def getCorrelation(signalA: DataStream[DataPoint[Double]], signalB: DataStream[DataPoint[Double]], labelA: String, labelB: String, windowSize: Long, windowSlide: Long) = {
    signalA.union(signalB)
      .map(t => {
      var dataPoint = new DataPoint[Double](t.t, t.label, t.value)
      dataPoint.key = "1"
      dataPoint
    }).keyBy(t => t.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(windowSize), Time.minutes(windowSlide)))
      .process(new Correlation(labelA, labelB, windowSizeInMinutes = windowSize))
  }

  def processSignal(signal: DataStream[DataPoint[Double]],
                    label: String,
                    windowSize: Long,
                    windowSlide: Long):
  DataStream[DataPoint[Double]] = {
    signal
      .filter(t => t.label == label)
      .keyBy(t => t.label)
      .window(SlidingEventTimeWindows.of(Time.minutes(windowSize), Time.minutes(windowSlide)))
      .process(new Interpolation(mode="linear", windowSizeInMinutes = windowSize))
      .keyBy(t => t.label)
      .map(new ZScoreCalculation())
      .filter(t => t.zScore > -3 && t.zScore < 3)
  }

}
