//package dfki.examples;
//
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
//import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
//public class LSTMTest {
//
//    public static void main(String[] args) throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        String fileName = "./seq_classification/python/train_seq_data.csv";
//        String lstmModelDir = "./seq_classification/python/lstm_model_vitals";
//
//        DataStream<Tuple4<String, Date, String, Double[]>> vitals = env.addSource(new ArrayReaderSourceFuntion(fileName))
//                .assignTimestampsAndWatermarks(new timeStampAssigner());
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
