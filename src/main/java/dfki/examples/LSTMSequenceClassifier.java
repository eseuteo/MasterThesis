package dfki.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.tensorflow.*;
import org.tensorflow.framework.MetaGraphDef;
import org.tensorflow.framework.SignatureDef;
import org.tensorflow.framework.TensorInfo;
import org.tensorflow.framework.TensorShapeProto;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Window function to classify a sequence of events contained in a window
 * using a pre-trained LSTM model
 * Input:
 *     Tuple4<key, Date, label, features[]>
 * Output:
 *    Tuple5<win start date, win end date, class label, prob class1, prob class2>
 *    Tuple5<String, String, String, Double, Double)
 */
public class LSTMSequenceClassifier extends ProcessWindowFunction<Tuple4<String, Date, String, Double[]>,
        Tuple5<String, String, String, Double, Double>, Tuple, TimeWindow> implements Serializable {
    private static final long serialVersionUID = 1L;

    private String modelFileName;
    private String firstInput;
    private String firstOutput;

    public LSTMSequenceClassifier(String modelFileName) throws IOException {
        this.modelFileName = modelFileName;

        // Get input and output string operation definitions from the metadata
        // we should get something like:
        //   firstInput: serving_default_bidirectional_input:0
        //   firstOutput: StatefulPartitionedCall:0
        //   see:
        //      https://stackoverflow.com/questions/61923351/how-to-invoke-model-from-tensorflow-java/61968561#61968561
        SavedModelBundle module = SavedModelBundle.load(modelFileName, "serve");
        MetaGraphDef metadata = MetaGraphDef.parseFrom(module.metaGraphDef());

        Map<String, Shape> nameToInput = getInputToShape(metadata);
        this.firstInput = nameToInput.keySet().iterator().next();

        Map<String, Shape> nameToOutput = getOutputToShape(metadata);
        this.firstOutput = nameToOutput.keySet().iterator().next();
        System.out.println("LSTM model: " + modelFileName + "  firstInput: " + firstInput + "  firstOutput: " + firstOutput);

    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple4<String, Date, String, Double[]>> iterable,
                        Collector<Tuple5<String, String, String, Double, Double>> collector) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");

        ArrayList<Double[]> events = new ArrayList<>();
        // create an array with the days in the sequence
        String sequence = "";
        // Tuple4<key, Date, label, features[]>
        for (Tuple4<String, Date, String, Double[]> in : iterable) {
            events.add(in.f3);
            sequence += "              " + sdf.format(in.f1) + "\n";
        }
        String winStart = sdf.format(new Date(context.window().getStart()));
        String winEnd = sdf.format(new Date(context.window().getEnd()));
        System.out.println("\nNEW SEQUENCE:\nWindow start: " + winStart + "\n" +
                sequence +
                "Window end:   " + winEnd);

        // create a matrix with the sequence features
        float[][] obs = new float[events.size()][events.get(0).length];
        for (int i = 0; i < events.size(); i++)
            for (int j = 0; j < events.get(0).length; j++)
                obs[i][j] = Float.parseFloat(events.get(i)[j] + "f");

        // classify sequence
        //https://riptutorial.com/tensorflow/example/32154/load-and-use-the-model-in-java-
        try (SavedModelBundle module = SavedModelBundle.load(modelFileName, "serve")) {

            // create an input Tensor
            Tensor input = Tensors.create(new float[][][]{obs});

            // create the session from the Bundle
            Session session = module.session();

            // run the model and get the result
            float[][] result = session
                    .runner()
                    .feed(firstInput, input)  // firstInput: "serving_default_bidirectional_input:0"
                    .fetch(firstOutput)       // firstOutput: "StatefulPartitionedCall:0"
                    .run()
                    .get(0)
                    .copyTo(new float[1][2]);

            // print out the result.
            System.out.println("lstm classifier output: " + Arrays.deepToString(result));

            String label = "Nonshock";
            if(result[0][0] > result[0][1]) {
                label = "shock";
            }


            // Returns:
            // win start, win end, probability of class 0, probability of class 1
            collector.collect(new Tuple5<String, String, String, Double, Double>(winStart, winEnd, label, (double) result[0][0], (double) result[0][1]));
        }
    }


    /**
     * @param metadata the graph metadata
     * @return the first signature, or null
     */
    private static SignatureDef getFirstSignature(MetaGraphDef metadata) {
        Map<String, SignatureDef> nameToSignature = metadata.getSignatureDefMap();
        if (nameToSignature.isEmpty())
            return null;
        return nameToSignature.get(nameToSignature.keySet().iterator().next());
    }

    /**
     * @param metadata the graph metadata
     * @return the output signature
     */
    private static SignatureDef getServingSignature(MetaGraphDef metadata) {
        return metadata.getSignatureDefOrDefault("serving_default", getFirstSignature(metadata));
    }

    /**
     * @param metadata the graph metadata
     * @return a map from an output name to its shape
     */
    protected static Map<String, Shape> getOutputToShape(MetaGraphDef metadata) {
        Map<String, Shape> result = new HashMap<>();
        SignatureDef servingDefault = getServingSignature(metadata);
        for (Map.Entry<String, TensorInfo> entry : servingDefault.getOutputsMap().entrySet()) {
            TensorShapeProto shapeProto = entry.getValue().getTensorShape();
            List<TensorShapeProto.Dim> dimensions = shapeProto.getDimList();
            long firstDimension = dimensions.get(0).getSize();
            long[] remainingDimensions = dimensions.stream().skip(1).mapToLong(TensorShapeProto.Dim::getSize).toArray();
            Shape shape = Shape.make(firstDimension, remainingDimensions);
            result.put(entry.getValue().getName(), shape);
        }
        return result;
    }

    /**
     * @param metadata the graph metadata
     * @return a map from an input name to its shape
     */
    protected static Map<String, Shape> getInputToShape(MetaGraphDef metadata) {
        Map<String, Shape> result = new HashMap<>();
        SignatureDef servingDefault = getServingSignature(metadata);
        for (Map.Entry<String, TensorInfo> entry : servingDefault.getInputsMap().entrySet()) {
            TensorShapeProto shapeProto = entry.getValue().getTensorShape();
            List<TensorShapeProto.Dim> dimensions = shapeProto.getDimList();
            long firstDimension = dimensions.get(0).getSize();
            long[] remainingDimensions = dimensions.stream().skip(1).mapToLong(TensorShapeProto.Dim::getSize).toArray();
            Shape shape = Shape.make(firstDimension, remainingDimensions);
            result.put(entry.getValue().getName(), shape);
        }
        return result;
    }


}
