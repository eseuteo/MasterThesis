package dfki.util;


import dfki.data.KeyedDataPoint;
import dfki.data.MimicWaveData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class UserDefinedFunctions {

    /**
     * Z-score normalisation, this zscore is applied online, thus we need to calculate meanx and sdx for every new
     * point in the stream, we keep and update those values in the state, and every time returns x[n]-meanx/sdx
     * sumx <- 0
     * sumx_minus_meanx <- 0
     * for(n in 1:length(x)) {
     *    print(x[n]
     *    sumx <- sumx + x[n]
     *    meanx <- sumx/n
     *    sumx_minus_meanx <- sumx_minus_meanx + (x[n]-meanx)^2
     *    sdx <- sqrt(sumx_minus_meanx/n)
     *    zscore_x[n] <- (x[n]-meanx)/sdx
     * }
     * see: https://en.wikipedia.org/wiki/Standard_score
     */
    public static class ZscoreNormalization extends RichMapFunction<KeyedDataPoint<Double>,KeyedDataPoint<Double>> {

        /** The state for the current key. */
        private transient ValueState<Tuple3<Integer, Double, Double>> zscore;

        @Override
        public void open(Configuration conf) {
            // get access to the state object
            zscore = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("zscore", Types.TUPLE()));
        }

        @Override
        public KeyedDataPoint<Double> map(KeyedDataPoint<Double> input) throws Exception {

            double meanx, sdx, sumx, sumx_minus_meanx, zscore_val = 0.0;
            int n;

            Tuple3<Integer, Double, Double> state = zscore.value();
            // the first time state is null, then it can be initialised
            if(state==null){
                state = new Tuple3<Integer,Double,Double>(1,0.0,0.0);
            }
            // access the state values
            n = state.f0;
            // sumx <- sumx + x[n]
            sumx = state.f1 + input.getValue();  // update sumx
            // meanx <- sumx/n
            meanx = sumx/n;
            // sumx_minus_meanx <- sumx_minus_meanx + (x[n]-meanx)^2
            sumx_minus_meanx = state.f2 + ((input.getValue()-meanx) * (input.getValue()-meanx));  // update
            // sdx <- sqrt(sumx_minus_meanx/n)
            sdx = Math.sqrt(sumx_minus_meanx/n);

            // with these meanx and sdx we can calculate the zscore for input.f1
            // zscore_x[n] <- (x[n]-meanx)/sdx
            if(sdx > 0 ) {
                zscore_val = (input.getValue() - meanx) / sdx;
            } else {
                if(meanx > 0) {
                    zscore_val = (input.getValue() - meanx);
                }
            }

            // Update state values
            n++;
            zscore.update(new Tuple3<>(n,sumx,sumx_minus_meanx));

            // returns KeyedDataPoint: (key,timestamp,value)
            return new KeyedDataPoint<Double>(input.getKey(), input.getTimeStampMs(), zscore_val);
        }

    }




    /**
     * This function extracts the time stamp from a mimic event
     */
    public static class ExtractTimestampMimic extends AscendingTimestampExtractor<MimicWaveData> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(MimicWaveData element) {
            return element.getTimeStampMs();
        }
    }

    /**
     * This function extracts the time stamp from a mimic event
     */
    public static class ExtractTimestampKeyedDataPoint extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
            return element.getTimeStampMs();
        }
    }

}
