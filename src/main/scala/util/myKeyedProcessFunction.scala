package util

import data.KeyedDataPoint
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

case class myKeyedProcessFunction() extends KeyedProcessFunction[Long, KeyedDataPoint[Double], KeyedDataPoint[Double]] {
  override def processElement(i: KeyedDataPoint[Double],
                              context: KeyedProcessFunction[Long, KeyedDataPoint[Double], KeyedDataPoint[Double]]#Context,
                              collector: Collector[KeyedDataPoint[Double]]): Unit = {

  }
}
