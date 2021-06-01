package util.featureextraction

import data.DataPoint
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class MultiScaleEntropy(r: Double, tau: Int, mMax: Int) extends ProcessWindowFunction [DataPoint[Double], (Long, Int, Int, Double), String, TimeWindow] {
  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[(Long, Int, Int, Double)]): Unit = {

    val avg = elements.map(t => t.value).sum / elements.size
    val variance = elements.map(t => t.value).map(a => math.pow(a - avg, 2)).sum / elements.size
    val stdev: Double = math.sqrt(variance)

    var listSignals = new ListBuffer[List[Double]]()
    var listMSE = new ListBuffer[ListBuffer[Double]]
    val signalList: List[Double] = elements.map(t => t.value).toList

    for (i <- 1 to tau) {
      listSignals += signalList.grouped(i).map(t => t.sum / i).toList
    }

    for (signal <- listSignals) {
      listMSE += getSampleEntropy(signal, r, stdev, tau, mMax)
    }

    for (i <- 0 until listMSE.length) {
      for (j <- 0 until listMSE(i).length) {
        out.collect((context.window.getStart, i, j, listMSE(i)(j)))
      }
    }
  }

  def getSampleEntropy(signal: List[Double], r: Double, stdev: Double, tau: Int, mMax: Int): ListBuffer[Double] = {
    val tolerance: Double = r * stdev
    var cont: ListBuffer[Int] = ListBuffer.fill(mMax + 2)(0)

    for (i: Int <- 0 until signal.length - mMax) {
      for (j: Int <- (i + 1) until signal.length - mMax) {
        var k: Int = 0
        while (k < mMax && math.abs(signal(i+k) - signal(j+k)) <= tolerance) {
          k += 1
          cont(k) += 1
        }
        if (k == mMax && math.abs(signal(i + mMax) - signal(j + mMax)) <= tolerance) {
          cont(mMax + 1) += 1
        }
      }
    }

    var sampleEntropy: ListBuffer[Double] = new ListBuffer[Double]()
    for (i <- 1 until mMax) {
      if (cont(i + 1) == 0 || cont(i) == 0) {
        sampleEntropy += -math.log(1d/((signal.length-mMax).toDouble * ((signal.length-mMax).toDouble - 1d)))
      } else {
        sampleEntropy += -math.log(cont(i + 1).toDouble/cont(i).toDouble)
      }
    }
    sampleEntropy
  }


}
