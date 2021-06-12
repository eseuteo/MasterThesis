package util.featureextraction

import data.DataPoint
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Correlation(signalA: String, signalB: String) extends ProcessWindowFunction[DataPoint[Double], Double, String, TimeWindow] {

  override def process(key: String, context: Context,
                       elements: Iterable[DataPoint[Double]],
                       out: Collector[Double]): Unit = {
    val listA = elements.filter(elem => elem.label.equals(signalA))
    val listB = elements.filter(elem => elem.label.equals(signalB))

    val parametersA = generateParameters(listA)
    val parametersB = generateParameters(listB)

    val x1 = parametersA._1
    val y1 = parametersA._2

    val x2 = parametersB._1
    val y2 = parametersB._2

    val otherParameters = generateParameters2(listA, listB)
    val diff_1 = otherParameters._1
    val oth_1 = otherParameters._2
    val totalSize = diff_1 + oth_1
    val t1 = Math.abs(x1 - y1)
    val t2 = Math.abs(x2 - y2)

    val h1 = x1
    val l1 = y1
    val h2 = x2
    val l2 = y2
    val s = diff_1
    val d = oth_1

    val w = getWValue(h1, l1, h2, l2, s, d)

    val correlation: Double = (Math.max(diff_1, oth_1) - Math.abs(t1 - t2)) * 1.0 / totalSize
    out.collect(correlation)
  }

  def getWValue(h1: Int, l1: Int, h2: Int, l2: Int, s: Int, d: Int): Int = {
    val a = math.abs(h1 - l1)
    val b = math.abs(h2 - l2)
    val c = math.abs(s - d)
    if (a == b == c) {
      if ((a + b) / 2 > (s - d)) {
        -1
      } else {
        1
      }
    } else if (a == b) {
      if ((a + b) / 2 > c) {
        -1
      } else {
        1
      }
    } else {
      val aux = math.abs(math.max((h1 - l1), (h2 - l2))) + math.min((h1 - l1), (h2 - l2))
      if (aux > c) {
        -1
      } else {
        1
      }
    }
  }

  def generateParameters(listValues : Iterable[DataPoint[Double]]): (Int, Int) = {
    var x = 0
    var y = 0

    var prev: Double = -1.0
    listValues.foreach(elem => {
      if (prev != -1.0) {
        if (prev < elem.value) {
          x += 1
        } else {
          y += 1
        }
      }
      prev = elem.value
    })
    (x, y)
  }

  def generateParameters2(listA: Iterable[DataPoint[Double]], listB: Iterable[DataPoint[Double]]): (Int, Int) = {
    var diff_1 = 0
    var oth_1 = 0

    var prevA: Double = -1.0
    var prevB: Double = -1.0
    listA.zip(listB).foreach(elem => {
      if (prevA != -1.0 && prevB != -1.0) {
        if (prevA < elem._1.value && prevB < elem._2.value || prevA > elem._1.value && prevB > elem._2.value) {
          diff_1 += 1
        } else {
          oth_1 += 1
        }
      }
      prevA = elem._1.value
      prevB = elem._2.value
    })
    (diff_1, oth_1)
  }
}
