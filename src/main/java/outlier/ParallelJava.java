//package outlier;
//
//import dfki.data.DataPoint;
//import dfki.data.KeyedDataPoint;
//import dfki.data.MimicWaveData;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.java.tuple.Tuple;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class ParallelJava extends ProcessWindowFunction<MimicWaveData, MimicWaveData, String, TimeWindow> {
////    private Integer timeSlide;
////    private Double range;
////    private Integer k;
//
////    public ParallelJava(Integer timeSlide, Double range, Integer k) {
////        this.timeSlide = timeSlide;
////        this.range = range;
////        this.k = k;
////    }
//
////    @Override
////    public void process(Tuple key, Context context, Iterable<KeyedDataPoint<Double>> elements,
////                        Collector<KeyedDataPoint<Double>> out) throws Exception {
////        Integer timeSlide = 10;
////        Double range = 5.0;
////        Integer k = 5;
////        TimeWindow window = context.window();
////        List<Tuple2<Boolean, KeyedDataPoint<Double>>> inputList = new ArrayList<>();
////        elements.forEach(element -> inputList.add(new Tuple2<>(false, element)));
////
////        inputList.removeIf(element -> element.f1.getTimeStampMs() < window.getEnd() - timeSlide);
////        inputList.forEach(
////                element -> {
////                    refreshList(element, inputList, window);
////                }
////        );
////
////        inputList.forEach(element -> {
////            if (element.f0) {
////                out.collect(element.f1);
////            }
////        });
////    }
//
//    private void refreshList(Tuple2<Boolean, MimicWaveData> node, List<Tuple2<Boolean,
//            MimicWaveData>> nodes,
//                             TimeWindow window) {
//        Double range = 5.0;
//        if (!nodes.isEmpty()) {
//            List<Tuple2<Boolean, MimicWaveData>> neighbors = new ArrayList<>(nodes);
//            neighbors.removeIf(element -> element.f1.getKey().equals(node.f1.getKey()));
//            List<Tuple3<Boolean, MimicWaveData, Double>> distances = new ArrayList<>();
//            neighbors.forEach(element -> {
//               distances.add(new Tuple3<>(element.f0, element.f1,
//                       Math.abs(element.f1. - node.f1.getValue())));
//            });
//            distances.removeIf(element -> element.f2 > range);
//
//            Integer neighborCount = distances.size();
//
//            node.f0 = neighborCount > 0;
//        }
//    }
//
//    @Override
//    public void process(String s, Context context, Iterable<MimicWaveData> elements,
//                        Collector<MimicWaveData> out) throws Exception {
//        Integer timeSlide = 10;
//        Double range = 5.0;
//        Integer k = 5;
//        TimeWindow window = context.window();
//        List<Tuple2<Boolean, MimicWaveData>> inputList = new ArrayList<>();
//        elements.forEach(element -> inputList.add(new Tuple2<>(false, element)));
//
//        inputList.removeIf(element -> element.f1.getTimeStampMs() < window.getEnd() - timeSlide);
//        inputList.forEach(
//                element -> {
//                    refreshList(element, inputList, window);
//                }
//        );
//
//        inputList.forEach(element -> {
//            if (element.f0) {
//                out.collect(element.f1);
//            }
//        });
//    }
//}
