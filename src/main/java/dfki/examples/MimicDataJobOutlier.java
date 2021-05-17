package dfki.examples;

import dfki.connectors.sinks.SerializationFunctions.KeyedDataPointDataSerialization;
import dfki.connectors.sources.MimicDataSourceFunction;
import dfki.data.KeyedDataPoint;
import dfki.util.UserDefinedFunctions;
import dfki.util.UserDefinedWindowFunctions;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * run with these parameters:
 * --input ./src/main/resources/a40834n.csv
 * --orderMA 10
 * --slideMA 1
 * --output signal.csv
 * --kafkaTopic mimicdata
 * The data in the example has one value per minute, so here orderMA 10 means average
 * every 10 minutes
 * For visualization:
 * in kafka consola create a topic mimicdata (if not created already)
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic
 * mimicdata
 * start in visual:
 * node kafka_consumer.js mimicdata
 * start from visual:
 * firefox MimicDataJob.html
 */
public class MimicDataJobOutlier {
    public static void main(String[] args) throws Exception {
        // Input parameters parsing
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        if (!parameters.has("input")) {
            throw new Exception("Input Data is not specified");
        }

        String mimicFile = parameters.get("input");
        String outCsvFile = parameters.get("output");
        String kafkaTopic = parameters.get("kafkaTopic");

        int orderMA = Integer.valueOf(parameters.get("orderMA"));
        int slideMA = Integer.valueOf(parameters.get("slideMA"));
        System.out.println("  Input file: " + mimicFile);
        System.out.println("  Result sink in kafka topic: " + kafkaTopic);


        // Properties for writing stream in Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<KeyedDataPoint<Double>> MimicDataKafkaProducer = new FlinkKafkaProducer<>(kafkaTopic,
                // target topic
                new KeyedDataPointDataSerialization(),       // serialization schema
                properties,                                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);   // fault-tolerance

        // Flink process start
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        @SuppressWarnings({"rawtypes", "serial"})


        // test with file in: src/main/resources/a40834n.csv
        DataStream<KeyedDataPoint<Double>> mimicData =
                env.addSource(new MimicDataSourceFunction(mimicFile)).assignTimestampsAndWatermarks(new UserDefinedFunctions.ExtractTimestampKeyedDataPoint());

        //mimicData.print();
        //mimicData.addSink(MimicDataKafkaProducer);

        DataStream<KeyedDataPoint<Double>> mimicDataSmooth = mimicData.keyBy("key")
                // mimic data is sampled every minute
                // smooth by orderMA windows that slide every slideMA
                .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA))).trigger(CountTrigger.of(orderMA))
                .process(new UserDefinedWindowFunctions.MovingAverageFunction());

        //mimicDataSmooth.print();
        //mimicDataSmooth.addSink(MimicDataKafkaProducer);

        DataStream<KeyedDataPoint<Double>> mimicDataSmoothNormalized = mimicDataSmooth
                // apply z-score normalisation to each vital signal
                .keyBy("key").map(new UserDefinedFunctions.ZscoreNormalization());

        mimicDataSmoothNormalized.print();
        mimicDataSmoothNormalized.addSink(MimicDataKafkaProducer);

        System.out.println("  Result saved in file: " + outCsvFile);
        env.execute("MimicDataJob");
    }


}
