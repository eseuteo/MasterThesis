package dfki.connectors.sources;

import dfki.data.MimicWaveData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.lang.Thread.sleep;

public class MimicDirDataSourceFunction implements SourceFunction<MimicWaveData>{
    private volatile boolean isRunning = true;

    private String directoryPathName;



    /*
    During  initialisation read the list of files in the directory
    order the list according to name, the name has date and time, so to read according
    to the time they were created
     */
    public MimicDirDataSourceFunction(String directoryPathName) {
        this.directoryPathName = directoryPathName;
    }

    public void run(SourceFunction.SourceContext<MimicWaveData> sourceContext){
        List<Path> pathList = new ArrayList<>();
        File fileDirectoryPath = new File(directoryPathName);
        // Get the files of the directory sorted by name
        getFileNames(pathList, fileDirectoryPath);
        // CHECK if we need this in the mimic data ?
        // We need to sort the files... to get the data in order
        pathList.sort(Comparator.naturalOrder());
        System.out.println(" Number of files to process: " + pathList.size());

        // open each file and get line by line
        String line;
        try {
            for(Path path : pathList) {
                String patientID = path.getFileName().toString().replace(".csv","");
                System.out.println("   Reading patientID: " + patientID + "   path" + path.toString());

                // Read each file, line by line
                BufferedReader br = new BufferedReader(new FileReader(path.toFile()));
                // Now get the lines
                // The first and second line indicates which measurements each file contains
                // we need to find out which because not all the files have the same measurements in the same order
                // First line:
                line = br.readLine();
                HashMap<String, Integer> header = getVitalsHeaderOrder(line);
                // Second line:  is the measurement type
                line = br.readLine();
                while ((line = br.readLine()) != null) {
                    String rawData = line;
                    //System.out.println(" LINE: " + line);
                    String data[] = rawData.split(",");
                    // if a field contains only "-", replace it with 0.0
                    for(int i=0; i<data.length; i++){
                        if(data[i].contentEquals("-"))
                            data[i] = "0.0";
                    }
                    String var1 = data[0].replace("'[", "");
                    String var2 = var1.replace("]'", "");
                    /////////-----var2 = var2.replace("2015","2018");

                    long millisSinceEpoch = LocalDateTime.parse(var2, DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();

                    // val[0], val[1],...
                    // 'HR','ABPSys','ABPDias','ABPMean','PAPSys','PAPDias','PAPMean','CVP','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
                    double val[] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
                    if(header.containsKey("hr")) {
                        val[0] = Double.valueOf(data[header.get("hr")]);
                    }
                    if(header.containsKey("abpsys"))
                        val[1] = Double.valueOf(data[header.get("abpsys")]);
                    if(header.containsKey("abpdias"))
                        val[2] = Double.valueOf(data[header.get("abpdias")]);
                    if(header.containsKey("abpmean"))
                        val[3] = Double.valueOf(data[header.get("abpmean")]);
                    if(header.containsKey("papsys"))
                        val[4] = Double.valueOf(data[header.get("papsys")]);
                    if(header.containsKey("papdias"))
                        val[5] = Double.valueOf(data[header.get("papdias")]);
                    if(header.containsKey("papmean"))
                        val[6] = Double.valueOf(data[header.get("papmean")]);
                    if(header.containsKey("cvp"))
                        val[7] = Double.valueOf(data[header.get("cvp")]);
                    if(header.containsKey("pulse"))
                        val[8] = Double.valueOf(data[header.get("pulse")]);
                    if(header.containsKey("resp"))
                        val[9] = Double.valueOf(data[header.get("resp")]);
                    if(header.containsKey("spo2"))
                        val[10] = Double.valueOf(data[header.get("spo2")]);
                    if(header.containsKey("nbpsys"))
                        val[11] = Double.valueOf(data[header.get("nbpsys")]);
                    if(header.containsKey("nbpdias"))
                        val[12] = Double.valueOf(data[header.get("nbpdias")]);
                    if(header.containsKey("nbpmean"))
                        val[13] = Double.valueOf(data[header.get("nbpmean")]);

                    MimicWaveData event = new MimicWaveData("mimic2wdb",
                            patientID,
                            Double.valueOf(val[0]),  // HR
                            Double.valueOf(val[1]),  // ABPSys
                            Double.valueOf(val[2]),  // ABPDias
                            Double.valueOf(val[3]),  // ABPMean
                            Double.valueOf(val[4]),  // PAPSys
                            Double.valueOf(val[5]),  // PAPDias
                            Double.valueOf(val[6]),  // PAPMean
                            Double.valueOf(val[7]),  // CVP
                            Double.valueOf(val[8]),  // Pulse
                            Double.valueOf(val[9]), // Resp
                            Double.valueOf(val[10]), // SpO2
                            Double.valueOf(val[11]), // NBPSys
                            Double.valueOf(val[12]), // NBPDias
                            Double.valueOf(val[13]), // NBPMean
                            millisSinceEpoch);

                    sourceContext.collect(event);

                }
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cancel() {
        this.isRunning = false;
    }

    /**
     *
     */
    private List<Path> getFileNames(List<Path> pathList, File fileDirectoryPath) {
        try {
            File[] fileNames = fileDirectoryPath.listFiles();
            for (File csv : fileNames) {
                if(csv.isDirectory()) {
                    getFileNames(pathList, csv);
                } else {
                    pathList.add(csv.toPath());
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return pathList;
    }

    /**
     * Not all the signals might be available in each file, here some headers examples:
     * ==> a40473n.csv <==
     * 'Time and date','HR','ABPSys','ABPDias','ABPMean','CVP','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
     * ==> a40802n.csv <==
     * 'Time and date','HR','ABPSys','ABPDias','ABPMean','PAPSys','PAPDias','PAPMean','CVP','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
     * ==> a41434n.csv <==
     * 'Time and date','HR','ABPSys','ABPDias','ABPMean','PULSE','RESP','SpO2'
     * ==> a41934n.csv <==
     * 'Time and date','HR','ABPSys','ABPDias','ABPMean','PAPSys','PAPDias','PAPMean','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
     */
    private HashMap<String, Integer> getVitalsHeaderOrder(String line){

        String[] headerLine = line.split(",");
        HashMap<String, Integer> vitalsHeaderOrder = new HashMap<String, Integer>();
        String key;
        for(int i=0; i<headerLine.length; i++){
           key = headerLine[i].replace("'","").toLowerCase(Locale.ROOT);
           vitalsHeaderOrder.put(key,i);
        }
        System.out.println("   HEADER: " + vitalsHeaderOrder);
        return vitalsHeaderOrder;

    }

}
