package models

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.api.java.tuple.Tuple5
import org.tensorflow.framework.{MetaGraphDef, SignatureDef}
import org.tensorflow.{SavedModelBundle, Shape, Tensor, Tensors}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.Try

class LSTMSequenceClassifier(
    modelFileName: String
) extends ProcessWindowFunction[
      Tuple4[String, Long, String, Array[Double]],
      Tuple5[String, String, String, Double, Double],
      String,
      TimeWindow
    ] {

  override def process(
      key: String,
      context: Context,
      elements: Iterable[Tuple4[String, Long, String, Array[Double]]],
      out: Collector[Tuple5[String, String, String, Double, Double]]
  ): Unit = {
    var events: util.ArrayList[Array[Double]] =
      new util.ArrayList[Array[Double]]()

    val winStart = context.window.getStart
    val winEnd = context.window.getEnd

    var sequence: StringBuilder = new StringBuilder

    elements.foreach(tuple => {
      events.add(tuple.f3)
      sequence.append(s"${tuple.f1}\n")
    })

    var obs = Array.ofDim[Float](events.size(), events.get(0).length)
    for (i <- 0 until events.size()) {
      for (j <- 0 until events.get(0).length) {
        obs(i)(j) = events.get(i)(j).toFloat
      }
    }

    val bundle = SavedModelBundle.load(modelFileName, "serve")

    val sess = bundle.session()
    val input = Tensors.create(Array[Array[Array[Float]]](obs))
    val y = sess
      .runner()
      .feed("serving_default_bidirectional_input:0", input)
      .fetch("StatefulPartitionedCall:0")
      .run()
      .get(0)

    val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
    y.copyTo(result)

    if (input != null) input.close()
    if (y != null) y.close()
    if (sess != null) sess.close()
    if (bundle != null) bundle.close()

    var label = "Nonshock"
    if (result(0)(0) > result(0)(1)) {
      label = "shock"
    }

    val dateStart = new Date(winStart)
    val dateEnd = new Date(winEnd)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    out.collect(
      new Tuple5(
        sdf.format(dateStart),
        sdf.format(dateEnd),
        label,
        result(0)(0).toDouble,
        result(0)(1).toDouble
      )
    )
  }
}
