package dfki.examples;

import dfki.connectors.sinks.SerializationFunctions;
import dfki.connectors.sources.MimicDirDataSourceFunction;
import dfki.data.MimicWaveData;
import dfki.util.UserDefinedFunctions;
import dfki.util.UserDefinedWindowFunctions.MovingAverageArrayFunction;
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
 * Run with these parameters:
    --input ./src/main/resources/mimic2wdb/train-set/C1/
    --orderMA 60
    --slideMA 30
    --output signal.csv
    --kafkaTopic mimicdata
    The data in the example has one value per minute, so here orderMA 60 means average
    every 60 minutes sliding the window every 30 minutes
 * For visualization:
 *    in kafka consola create a topic mimicdata (if not created already)
 *       bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic mimicdata
 *    start in visual:
 *      node kafka_consumer.js mimicdata
 *    start from visual:
 *      firefox MimicDataArrayJob.html
 */
public class MimicDataArrayJob {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String mimicDirectory = parameters.get("input");
        String outCsvFile = parameters.get("output");
        int orderMA = Integer.valueOf(parameters.get("orderMA"));
        int slideMA = Integer.valueOf(parameters.get("slideMA"));
        String kafkaTopic = parameters.get("kafkaTopic");
        System.out.println("  Processing files in directory: " + mimicDirectory);

        // Properties for writing stream in Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<MimicWaveData> MimicDataKafkaProducer = new FlinkKafkaProducer<>(
                kafkaTopic,                                               // target topic
                new SerializationFunctions.MimicWaveDataSerialization(),  // serialization schema
                properties,                                               // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);                // fault-tolerance


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        @SuppressWarnings({"rawtypes", "serial"})

        // test with file in: src/main/resources/a40834n.csv
        DataStream<MimicWaveData> mimicData = env
                .addSource(new MimicDirDataSourceFunction(mimicDirectory))
                .assignTimestampsAndWatermarks(new UserDefinedFunctions.ExtractTimestampMimic());

        //mimicData.print();

        DataStream<MimicWaveData> mimicDataSmooth = mimicData.keyBy(MimicWaveData::getRecordId)
                // mimic data is sampled every minute
                // here we apply a moving average function to each of the vitals
                //.window(SlidingEventTimeWindows.of(Time.seconds(orderMA), Time.seconds(slideMA)))
                .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
                .trigger(CountTrigger.of(orderMA))
                // Apply a moving average window to smooth the data
                .process(new MovingAverageArrayFunction());

        // simple print in consola of smothed data
        mimicDataSmooth.print();

        // sink in kafka for visualization
        mimicDataSmooth.addSink(MimicDataKafkaProducer);


        System.out.println("  Result saved in file: " + outCsvFile);
        env.execute("MimicDataJob");
    }


}
