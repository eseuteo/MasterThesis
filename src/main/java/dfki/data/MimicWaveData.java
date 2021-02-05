package dfki.data;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MimicWaveData {
    public String key;
    private String recordId;
    private Double HR;
    private Double ABPSys;
    private Double ABPDias;
    private Double ABPMean;
    private Double PAPSys;
    private Double PAPDias;
    private Double PAPMean;
    private Double CVP;
    private Double Pulse;
    private Double Resp;
    private Double SpO2;
    private Double NBPSys;
    private Double NBPDias;
    private Double NBPMean;
    private Long timeStampMs;
    private Double resultCEP;

    public MimicWaveData(){ }

    // 'HR','ABPSys','ABPDias','ABPMean','PAPSys','PAPDias','PAPMean','CVP','PULSE','RESP','SpO2','NBPSys','NBPDias','NBPMean'
    public MimicWaveData(
            String key,
            String recordId,
            Double HR,
            Double ABPSys,
            Double ABPDias,
            Double ABPMean,
            Double PAPSys,
            Double PAPDias,
            Double PAPMean,
            Double CVP,
            Double Pulse,
            Double Resp,
            Double SpO2,
            Double NBPSys,
            Double NBPDias,
            Double NBPMean,
            Long timeStampMs){
        this.key = key;
        this.recordId = recordId;
        this.HR = HR;
        this.ABPSys = ABPSys;
        this.ABPDias = ABPDias;
        this.ABPMean = ABPMean;
        this.PAPSys = PAPSys;
        this.PAPDias = PAPDias;
        this.PAPMean = PAPMean;
        this.CVP = CVP;
        this.Pulse = Pulse;
        this.Resp = Resp;
        this.SpO2 = SpO2;
        this.NBPSys = NBPSys;
        this.NBPDias = NBPDias;
        this.NBPMean = NBPMean;
        this.timeStampMs = timeStampMs;
        this.resultCEP= 0.0;  // just the CEP result will set up this value
    }

    public String getKey() { return key; }
    public void setKey(String val) { this.key = val; }

    public String getRecordId() { return recordId; }
    public void setRecordId(String recordId) { this.recordId = recordId; }

    public Double getHR() { return HR; }
    public void setHR(Double HR) {this.HR = HR; }

    public void setABPSys(Double ABPSys) { this.ABPSys = ABPSys; }
    public Double getABPSys() {return ABPSys;}

    public void setABPDias(Double ABPDias) { this.ABPDias = ABPDias; }
    public Double getABPDias() { return ABPDias; }

    public Double getABPMean() { return ABPMean; }
    public void setABPMean(Double ABPMean) { this.ABPMean = ABPMean; }

    public void setPAPSys(Double PAPSys) {this.PAPSys = PAPSys; }
    public Double getPAPSys() { return PAPSys; }

    public void setPAPDias(Double PAPDias) {this.PAPDias = PAPDias; }
    public Double getPAPDias() {return PAPDias; }

    public Double getPAPMean() { return PAPMean; }
    public void setPAPMean(Double PAPMean) { this.PAPMean = PAPMean; }

    public Double getCVP() { return CVP; }
    public void setCVP(Double CVP) { this.CVP = CVP; }

    public Double getPulse() { return Pulse; }
    public void setPulse(Double pulse) { Pulse = pulse; }

    public Double getResp() { return Resp; }
    public void setResp(Double resp) { Resp = resp; }

    public Double getSpO2() { return SpO2; }
    public void setSpO2(Double spO2) { SpO2 = spO2; }

    public void setNBPSys(Double NBPSys) {this.NBPSys = NBPSys; }
    public Double getNBPSys() {return NBPSys; }

    public void setNBPDias(Double NBPDias) {this.NBPDias = NBPDias; }
    public Double getNBPDias() { return NBPDias; }

    public Double getNBPMean() { return NBPMean; }
    public void setNBPMean(Double NBPMean) { this.NBPMean = NBPMean; }

    public Long getTimeStampMs() { return timeStampMs; }
    public void setTimeStampMs(Long timeStampMs) { this.timeStampMs = timeStampMs; }

    public Double getResultCEP() { return resultCEP; }
    public void setResultCEP(Double resultCEP) { this.resultCEP = resultCEP; }

    //@Override
    public String toString(){
        Date date = new Date(this.timeStampMs);
        // to print the date in normal format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

       return sdf.format(date) + "," +     // val[0]
               this.getRecordId() + "," +
               this.getHR() + "," +        // val[1]
               this.getABPMean() + "," +   // val[2]
               this.getPAPMean() + "," +
               this.getCVP() + "," +
               this.getPulse() + "," +
               this.getResp() + "," +
               this.getSpO2() + "," +
               this.getNBPMean() + "," +
               this.getResultCEP();        // val[9]
    }

}
