//package outlier;
//
//import common_utils.Data;
//import dfki.data.MimicWaveData;
//import mtree.*;
//import mtree.utils.Pair;
//import mtree.utils.Utils;
//import org.apache.flink.api.common.state.KeyedStateStore;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//
//import java.util.Set;
//
//public class AdvancedJava extends ProcessWindowFunction<MimicWaveData, MimicWaveData, String, TimeWindow> {
//
//    private transient ValueState<AdvancedState> state;
//
//    @Override
//    public void process(String s, Context context, Iterable<MimicWaveData> iterable, Collector<MimicWaveData> collector) throws Exception {
//        TimeWindow window = context.window();
//        AdvancedState current = state.value();
//
//        if (current == null) {
//            PromotionFunction<Data> nonRandomPromotion = new PromotionFunction<Data>() {
//                @Override
//                public Pair<Data> process(Set<Data> data, DistanceFunction<? super Data> distanceFunction) {
//                    return Utils.minMax(data);
//                }
//            };
//            ComposedSplitFunction<Data> mySplit = new ComposedSplitFunction<Data>(nonRandomPromotion,
//                    new PartitionFunctions.BalancedPartition<Data>());
//            myTree = new MTree<Data>(k, DistanceFunctions.EUCLIDEAN, mySplit);
//        }
//
//    }
//}
