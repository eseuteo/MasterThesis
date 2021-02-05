package dfki.connectors.sinks;

import dfki.data.KeyedDataPoint;
import dfki.data.MimicWaveData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class SerializationFunctions {

    public static class StringSerialization implements KeyedSerializationSchema<Tuple2<String,String>> {
        @Override
        public byte[] serializeKey(Tuple2<String,String> s) {
            return null;
        }
        @Override
        public byte[] serializeValue(Tuple2<String,String> s) {
            return s.f1.getBytes();
        }
        @Override
        public String getTargetTopic(Tuple2<String,String> s) {
            return null;
        }
    }

    public static class MimicWaveDataSerialization implements KeyedSerializationSchema<MimicWaveData> {
        @Override
        public byte[] serializeKey(MimicWaveData s) {
            return null;
        }
        @Override
        public byte[] serializeValue(MimicWaveData s) {
            return s.toString().getBytes();
        }
        @Override
        public String getTargetTopic(MimicWaveData s) {
            return null;
        }
    }

    public static class KeyedDataPointDataSerialization implements KeyedSerializationSchema<KeyedDataPoint<Double>> {
        @Override
        public byte[] serializeKey(KeyedDataPoint<Double> s) {
            return null;
        }
        @Override
        public byte[] serializeValue(KeyedDataPoint<Double> s) {return s.toString().getBytes(); }
        @Override
        public String getTargetTopic(KeyedDataPoint<Double> s) { return null;  }
    }
}
