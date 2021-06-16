import data.DataPoint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import util.featureextraction.Correlation


class FeatureExtractionSpec extends AnyFlatSpec with Matchers {
  "Correlation" should "Obtain approximate correlation between two DataPoint[Double] DataStreams"
  val oneInputStreamOperator = new Correlation("signalA", "signalB")

  val testHarness = new KeyedOneInputStreamOperatorTestHarness(oneInputStreamOperator, new TupleKeySelector())
}
