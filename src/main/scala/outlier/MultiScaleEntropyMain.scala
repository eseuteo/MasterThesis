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
import util.featureextraction.{Correlation, MultiScaleEntropy}
import util.interpolation.{CustomInterpolation, Interpolation}
import util.{MovingAverageFunction, OutlierEvaluation, ZScoreCalculation, myKeyedProcessFunction}

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object MultiScaleEntropyMain {
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
        .foreach((item, signal) => list += new DataPoint[Double](timestamp, signal, item.toDouble))
      list
    }).assignTimestampsAndWatermarks(watermarkStrategy)

    val mse = mimicDataWithTimestamps
      .keyBy(t => t.label)
      .window(TumblingEventTimeWindows.of(Time.minutes(1000)))
      .process(new MultiScaleEntropy(0.001, 5, 5))

    mse.print()

    env.execute("MimicDataJob")

  }

}
