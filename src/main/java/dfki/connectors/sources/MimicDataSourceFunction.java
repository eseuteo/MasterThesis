package dfki.connectors.sources;

import dfki.data.KeyedDataPoint;
import dfki.data.MimicWaveData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.lang.Thread.sleep;


public class MimicDataSourceFunction implements SourceFunction<KeyedDataPoint<Double>> {
    private volatile boolean isRunning = true;

    private String mimic2wdbFile;  // wave file in .csv format

    /*
     Read the wave file from the .csv file
     */
    public MimicDataSourceFunction(String fileName) {
        this.mimic2wdbFile = fileName;
    }


    public void run(SourceFunction.SourceContext<KeyedDataPoint<Double>> sourceContext) throws Exception {

        // the data look like this... and we want to process ABPMean <- field 4
        // for this example I remove the first line...
        //                     0   1    2      3       4       5      6      7      8    9    10   11   12      13     14
        //            Timeanddate,HR,ABPSys,ABPDias,ABPMean,PAPSys,PAPDias,PAPMean,CVP,PULSE,RESP,SpO2,NBPSys,NBPDias,NBPMean,CO
        // '[10:36:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:37:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:38:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:39:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
        // '[10:40:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000

        // open each file and get line by line
        try {
            System.out.println("  FILE:" + mimic2wdbFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(mimic2wdbFile), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {

                String rawData = line;

                //String rawData = rawData0.replaceAll("-","0.0");
                //System.out.println("LINE0: " + rawData0 + "\nLINE1: " + rawData);
                String[] data = rawData.split(",");
                // if a file contains only "-", replace it with 0.0
                for(int i=0; i<data.length; i++){
                    if(data[i].contentEquals("-"))
                        data[i] = "0.0";
                }
                String var1 = data[0].replace("'[", "");
                String var2 = var1.replace("]'", "");

                long millisSinceEpoch = LocalDateTime.parse(var2, DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();

                sourceContext.collect(new KeyedDataPoint<Double>("HR", millisSinceEpoch, Double.valueOf(data[1])));
                sourceContext.collect(new KeyedDataPoint<Double>("ABPMean", millisSinceEpoch, Double.valueOf(data[4])));
                sourceContext.collect(new KeyedDataPoint<Double>("PAPMean", millisSinceEpoch, Double.valueOf(data[7])));
                sourceContext.collect(new KeyedDataPoint<Double>("CVP", millisSinceEpoch, Double.valueOf(data[8])));
                sourceContext.collect(new KeyedDataPoint<Double>("Pulse", millisSinceEpoch, Double.valueOf(data[9])));
                sourceContext.collect(new KeyedDataPoint<Double>("Resp", millisSinceEpoch, Double.valueOf(data[10])));
                sourceContext.collect(new KeyedDataPoint<Double>("SpO2", millisSinceEpoch, Double.valueOf(data[11])));
                sourceContext.collect(new KeyedDataPoint<Double>("NBPMean", millisSinceEpoch, Double.valueOf(data[14])));

            }
            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void cancel() {
        this.isRunning = false;
    }



}

