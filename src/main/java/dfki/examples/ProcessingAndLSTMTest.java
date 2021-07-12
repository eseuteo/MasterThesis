//package dfki.examples;
//
//import data.DataPoint;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.util.Collector;
//import util.interpolation.Interpolation;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.Properties;
//import java.util.stream.Stream;
//
//public class ProcessingAndLSTMTest {
//
//    public static void main(String[] args) throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        ParameterTool parameters =
//                ParameterTool.fromArgs(args);
//        String signals[] = {"HR", "ABPSys", "ABPDias", "ABPMean",
//                "RESP", "SpO2", "SOFA_SCORE"};
//
//        String mimicFile = parameters.getRequired("input");
//        String outCsvFile = parameters.getRequired("output");
//        String kafkaTopic = parameters.getRequired("kafkaTopic");
//
//        Long orderMA = Long.parseLong(parameters.getRequired(
//                "orderMA"));
//        Long slideMA = Long.parseLong(parameters.getRequired(
//                "slideMA"));
//        System.out.println("  Input file: " + mimicFile);
//        System.out.println("  Result sink in kafka topic: " + kafkaTopic);
//
//        // Properties for writing stream in Kafka
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092")
//
//        FlinkKafkaProducer kafkaProducer =
//                new FlinkKafkaProducer<String>(
//                kafkaTopic,
//                new SimpleStringSchema(),
//                properties);
//
//        StreamingFileSink<String> sink =
//                StreamingFileSink
//                .forRowFormat(new Path(outCsvFile),
//                        new SimpleStringEncoder<String>("UTF-8")).build();
//
//        env.setParallelism(1);
//        env.getConfig().enableObjectReuse();
//
//        DataStream<String> mimicData = env.readTextFile(mimicFile);
//
//        WatermarkStrategy<DataPoint<Double>> watermarkStrategy =
//                WatermarkStrategy.<DataPoint<Double>>forMonotonousTimestamps()
//                .withTimestampAssigner((event, timestamp) -> event.t());
//
//        DataStream<DataPoint<Double>> mimicDataWithTimestamps = mimicData
//                .flatMap(new FlatMapFunction<String, DataPoint<Double>>() {
//                    @Override
//                    public void flatMap(String s,
//                                        Collector<DataPoint<Double>> collector) throws Exception {
//                        String data[] = s.split(",");
//                        Long timestamp =
//                                LocalDateTime.parse(data[0], DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
//                                .atZone(ZoneId.systemDefault())
//                                .toInstant()
//                                .toEpochMilli();
//                        for (int i = 0; i < signals.length; i++) {
//                            collector.collect(new DataPoint<Double>(timestamp,
//                                    signals[i],
//                                    Double.parseDouble(data[i+1])));
//                        }
//                    }
//                }).assignTimestampsAndWatermarks(watermarkStrategy);
//
//        DataStream<DataPoint<Double>> hrProcessedSignal =
//                processSignal(mimicDataWithTimestamps, "HR", 60, 1);
//        val hrProcessedSignal = processSignal(mimicDataWithTimestamps, "HR", 60, 1)
//        val respProcessedSignal = processSignal(mimicDataWithTimestamps, "RESP", 60, 1)
//        val abpmProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPMean", 60, 1)
//        val abpsProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPSys", 60, 1)
//        val abpdProcessedSignal = processSignal(mimicDataWithTimestamps, "ABPDias", 60, 1)
//        val spo2ProcessedSignal = processSignal(mimicDataWithTimestamps, "SpO2", 60, 1)
//        val sofascore = processSignal(mimicDataWithTimestamps, "SOFA_SCORE", 60, 60)
//
//        val hrRespCorrelation = getCorrelation(hrProcessedSignal, respProcessedSignal, "HR", "RESP", orderMA, slideMA)
//        val hrAbpmeanCorrelation = getCorrelation(hrProcessedSignal, abpmProcessedSignal, "HR", "ABPMean", orderMA, slideMA)
//        val hrAbpsysCorrelation = getCorrelation(hrProcessedSignal, abpsProcessedSignal, "HR", "ABPSys", orderMA, slideMA)
//        val hrAbpdiasCorrelation = getCorrelation(hrProcessedSignal, abpdProcessedSignal, "HR", "ABPDias", orderMA, slideMA)
//        val hrSpo2Correlation = getCorrelation(hrProcessedSignal, spo2ProcessedSignal, "HR", "SpO2", orderMA, slideMA)
//        val respAbpmeanCorrelation = getCorrelation(respProcessedSignal, abpmProcessedSignal, "RESP", "ABPMean", orderMA, slideMA)
//        val respAbpsysCorrelation = getCorrelation(respProcessedSignal, abpsProcessedSignal, "RESP", "ABPSys", orderMA, slideMA)
//        val respAbpdiasCorrelation = getCorrelation(respProcessedSignal, abpdProcessedSignal, "RESP", "ABPDias", orderMA, slideMA)
//        val respSpo2Correlation = getCorrelation(respProcessedSignal, spo2ProcessedSignal, "RESP", "SpO2", orderMA, slideMA)
//        val abpmeanAbpsysCorrelation = getCorrelation(abpmProcessedSignal, abpsProcessedSignal, "ABPMean", "ABPSys", orderMA, slideMA)
//        val abpmeanAbpdiasCorrelation = getCorrelation(abpmProcessedSignal, abpdProcessedSignal, "ABPMean", "ABPDias", orderMA, slideMA)
//        val abpmeanSpo2Correlation = getCorrelation(abpmProcessedSignal, spo2ProcessedSignal, "ABPMean", "SpO2", orderMA, slideMA)
//        val abpsysAbpdiasCorrelation = getCorrelation(abpsProcessedSignal, abpdProcessedSignal, "ABPSys", "ABPDias", orderMA, slideMA)
//        val abpsysSpo2Correlation = getCorrelation(abpsProcessedSignal, spo2ProcessedSignal, "ABPSys", "SpO2", orderMA, slideMA)
//        val abpdiasSpo2Correlation = getCorrelation(abpdProcessedSignal, spo2ProcessedSignal, "ABPDias", "SpO2", orderMA, slideMA)
//
//        val sampleEntropyHR = getSampleEntropy(hrProcessedSignal, "HR", orderMA)
//        val sampleEntropyRESP = getSampleEntropy(respProcessedSignal, "RESP", orderMA)
//        val sampleEntropyABPMean = getSampleEntropy(abpmProcessedSignal, "ABPMean", orderMA)
//        val sampleEntropyABPSys = getSampleEntropy(abpsProcessedSignal, "ABPSys", orderMA)
//        val sampleEntropyABPDias = getSampleEntropy(abpdProcessedSignal, "ABPDias", orderMA)
//        val sampleEntropySpO2 = getSampleEntropy(spo2ProcessedSignal, "SpO2", orderMA)
//
//        val meanHR = getMean(hrProcessedSignal, orderMA, slideMA)
//        val stdevHR = getStdev(hrProcessedSignal, orderMA, slideMA)
//        val minHR = getMin(hrProcessedSignal, orderMA, slideMA)
//        val maxHR = getMax(hrProcessedSignal, orderMA, slideMA)
//
//        val meanRESP = getMean(respProcessedSignal, orderMA, slideMA)
//        val stdevRESP = getStdev(respProcessedSignal, orderMA, slideMA)
//        val minRESP = getMin(respProcessedSignal, orderMA, slideMA)
//        val maxRESP = getMax(respProcessedSignal, orderMA, slideMA)
//
//        val meanABPMean = getMean(abpmProcessedSignal, orderMA, slideMA)
//        val stdevABPMean = getStdev(abpmProcessedSignal, orderMA, slideMA)
//        val minABPMean = getMin(abpmProcessedSignal, orderMA, slideMA)
//        val maxABPMean = getMax(abpmProcessedSignal, orderMA, slideMA)
//
//        val meanABPSys = getMean(abpsProcessedSignal, orderMA, slideMA)
//        val stdevABPSys = getStdev(abpsProcessedSignal, orderMA, slideMA)
//        val minABPSys = getMin(abpsProcessedSignal, orderMA, slideMA)
//        val maxABPSys = getMax(abpsProcessedSignal, orderMA, slideMA)
//
//        val meanABPDias = getMean(abpdProcessedSignal, orderMA, slideMA)
//        val stdevABPDias = getStdev(abpdProcessedSignal, orderMA, slideMA)
//        val minABPDias = getMin(abpdProcessedSignal, orderMA, slideMA)
//        val maxABPDias = getMax(abpdProcessedSignal, orderMA, slideMA)
//
//        val meanSpO2 = getMean(spo2ProcessedSignal, orderMA, slideMA)
//        val stdevSpO2 = getStdev(spo2ProcessedSignal, orderMA, slideMA)
//        val minSpO2 = getMin(spo2ProcessedSignal, orderMA, slideMA)
//        val maxSpO2 = getMax(spo2ProcessedSignal, orderMA, slideMA)
//
//        hrRespCorrelation
//                .union(hrAbpmeanCorrelation)
//                .union(hrAbpsysCorrelation)
//                .union(hrAbpdiasCorrelation)
//                .union(hrSpo2Correlation)
//                .union(respAbpmeanCorrelation)
//                .union(respAbpsysCorrelation)
//                .union(respAbpdiasCorrelation)
//                .union(respSpo2Correlation)
//                .union(abpmeanAbpsysCorrelation)
//                .union(abpmeanAbpdiasCorrelation)
//                .union(abpmeanSpo2Correlation)
//                .union(abpsysAbpdiasCorrelation)
//                .union(abpsysSpo2Correlation)
//                .union(abpdiasSpo2Correlation)
//                .union(meanHR)
//                .union(stdevHR)
//                .union(minHR)
//                .union(maxHR)
//                .union(meanRESP)
//                .union(stdevRESP)
//                .union(minRESP)
//                .union(maxRESP)
//                .union(meanABPMean)
//                .union(stdevABPMean)
//                .union(minABPMean)
//                .union(maxABPMean)
//                .union(meanABPSys)
//                .union(stdevABPSys)
//                .union(minABPSys)
//                .union(maxABPSys)
//                .union(meanABPDias)
//                .union(stdevABPDias)
//                .union(minABPDias)
//                .union(maxABPDias)
//                .union(meanSpO2)
//                .union(stdevSpO2)
//                .union(minSpO2)
//                .union(maxSpO2)
//                .union(sampleEntropyHR)
//                .union(sampleEntropyRESP)
//                .union(sampleEntropyABPMean)
//                .union(sampleEntropyABPSys)
//                .union(sampleEntropyABPDias)
//                .union(sampleEntropySpO2)
//                .union(sofascore)
//                .keyBy(t => t.t)
//      .window(TumblingEventTimeWindows.of(Time.minutes(orderMA)))
//                .trigger(CountTrigger.of(46))
//                .process(GenerateSignalsMap())
//                .map(t => t._2)
//        .addSink(sink)
//
//        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        String fileName = "./seq_classification/python/train_seq_data.csv";
//        String lstmModelDir = "/home/ricardohb/Documents/Projects/FlinkSequences/seq_classification/python/lstm_model_vitals/";
//
//        DataStream<Tuple4<String, Date, String, Double[]>> vitals = env.addSource(new LSTMTest.ArrayReaderSourceFuntion(fileName))
//                .assignTimestampsAndWatermarks(new LSTMTest.timeStampAssigner());
//
//        DataStream<Tuple5<String, String, String, Double, Double>> sequenceClass =  vitals.keyBy(0)
//                .window(SlidingEventTimeWindows.of(Time.hours(3),Time.hours(1)))
//                // make sure that it will trigger just when the number of elelemnts is the expected window size
//                .trigger(CountTrigger.of(3))
//                // classify sequences of windowSize days
//                //            Tuple4<key, Date, label, features[]>
//                .process(new LSTMSequenceClassifier(lstmModelDir));
//
//        //vitals.print();
//        sequenceClass.print();
//
//
//        env.execute("LSTMTest");
//    }
//
//    private static DataStream<DataPoint<Double>> processSignal(DataStream<DataPoint<Double>> signal, String label, int orderMA,
//                                                               int slideMA) {
//        signal
//            .filter(new FilterFunction<DataPoint<Double>>() {
//                @Override
//                public boolean filter(DataPoint<Double> doubleDataPoint) throws Exception {
//                    return doubleDataPoint.value() != -1.0;
//                }})
//            .filter(new FilterFunction<DataPoint<Double>>() {
//                @Override
//                public boolean filter(DataPoint<Double> doubleDataPoint) throws Exception {
//                    return doubleDataPoint.label().equals(label);
//                }})
//            .keyBy(new KeySelector<DataPoint<Double>, Object>() {
//                @Override
//                public Object getKey(DataPoint<Double> doubleDataPoint) throws Exception {
//                    return doubleDataPoint.label();
//                }})
//            .window(SlidingEventTimeWindows.of(Time.minutes(orderMA), Time.minutes(slideMA)))
//            .process(new Interpolation("minutes", orderMA, "linear"))
//
//
//
//    // Tuple4<key, date, label, features[]>
//    static class ArrayReaderSourceFuntion implements SourceFunction<Tuple4<String, Date, String, Double[]>> {
//        private volatile boolean isRunning = true;
//
//        private String directoryFileName;
//
//        /*
//        During  initialisation read the list of files in the directory
//        order the list according to name, the name has date and time, so to read according
//        to the time they were created
//         */
//        public ArrayReaderSourceFuntion(String fileName) {
//            this.directoryFileName = fileName;
//        }
//
//
//        public void run(SourceFunction.SourceContext<Tuple4<String, Date, String, Double[]>> sourceContext) throws ParseException {
//            // open the csv file and get line by line
//            try {
//                System.out.println("  FILE:" + directoryFileName);
//
//                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(directoryFileName), "UTF-8"));
//                //LINE:index,DateVitals,hr,   abpSys,   abpDias,   abpMean,  resp,   sp02,    SDhr,   SDabpSys,   SDabpDias,   SDabpMean,  SDresp,   SDsp02,   SEhr,   SEresp,   CorHRabpSys,   CorHRabpDias,   CorHRabpMean,   CorHRresp,   CorHRsp02,   CorRespSp02,  sofa_Score, mylabel, Nonshock, shock
//                //        0     1       2       3                                                                                            12                                                                                                                21            22       23        24       25
//                //   sequence_cols = ['hr', 'abpSys', 'abpDias', 'abpMean', 'resp', 'sp02', 'SDhr', 'SDabpSys', 'SDabpDias', 'SDabpMean', 'SDresp', 'SDsp02', 'SEhr', 'SEresp', 'CorHRabpSys', 'CorHRabpDias', 'CorHRabpMean', 'CorHRresp', 'CorHRsp02', 'CorRespSp02']
//                String line;
//                // first line is the header
//                line = br.readLine();
//                //System.out.println("  HEADER:" + line);
//                while ((line = br.readLine()) != null) {
//                    //System.out.println("  LINE:" + line);
//                    String rawData = line;
//                    String field[] = rawData.split(",");
//
//                    // The features are from field 2 --> 21
//                    Double[] feas = new Double[20];
//                    for(int i=0; i<20; i++){
//                        feas[i] = Double.valueOf(field[i+2]);
//                    }
//                    // 2102-09-29 15:00:00
//                    Date date = new SimpleDateFormat("yyyy-MM-dd hh:mm:SS").parse(field[1]);
//                    String label = field[23];
//
//                    //System.out.println("  DATE:" + field[1] + " LABEL: " + label);
//
//                    // Tuple4<key, date, label, features[]>
//                    Tuple4<String, Date, String, Double[]> feasTuple = new Tuple4<String, Date, String, Double[]>("patientId",date,label,feas);
//                    sourceContext.collect(feasTuple);
//                }
//                br.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//
//        public void cancel() {
//            this.isRunning = false;
//        }
//
//
//    }
//
//
//    public static class timeStampAssigner implements AssignerWithPunctuatedWatermarks<Tuple4<String, Date, String, Double[]>> {
//        @Override
//        public long extractTimestamp(Tuple4<String, Date, String, Double[]> event, long previousElementTimestamp) {
//            return event.f1.getTime();
//        }
//
//        @Override
//        public Watermark checkAndGetNextWatermark(Tuple4<String, Date, String, Double[]> event, long extractedTimestamp) {
//            return new Watermark(extractedTimestamp);
//        }
//    }
//}
