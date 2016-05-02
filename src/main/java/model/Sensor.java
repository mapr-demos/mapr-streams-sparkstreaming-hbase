package model;

import java.util.Objects;
import org.apache.hadoop.hbase.util.Bytes;

public class Sensor {

    public String resid;
    public String date;
    public String time;
    public Double hz;
    public Double disp;
    public Double flo;
    public Double sedPPM;
    public Double psi;
    public Double chlPPM;

    public Sensor(String str) {
        String[] p = str.split(",");
        this.resid = p[0];
        this.date = p[1];
        this.time = p[2];
        this.hz = Double.parseDouble(p[3]);
        this.disp = Double.parseDouble(p[4]);
        this.flo = Double.parseDouble(p[5]);
        this.sedPPM = Double.parseDouble(p[6]);
        this.psi = Double.parseDouble(p[7]);
        this.chlPPM = Double.parseDouble(p[8]);
    }

    public Sensor(String resid, String date, String time, Double hz, Double disp, Double flo, Double sedPPM, Double psi, Double chlPPM) {
        this.resid = resid;
        this.date = date;
        this.time = time;
        this.hz = hz;
        this.disp = disp;
        this.flo = flo;
        this.sedPPM = sedPPM;
        this.psi = psi;
        this.chlPPM = chlPPM;
    }

    public Sensor(String rowkey, Double hz, Double disp, Double flo, Double sedPPM, Double psi, Double chlPPM) {
        String[] temp1 = rowkey.split("_");
        String[] temp2 = temp1[1].split(" ");
        this.resid = temp1[0];
        this.date = temp2[0];
        this.time = temp2[1];
        this.hz = hz;
        this.disp = disp;
        this.flo = flo;
        this.sedPPM = sedPPM;
        this.psi = psi;
        this.chlPPM = chlPPM;
    }

    @Override
    public String toString() {
        return "Sensor{" + "resid=" + resid + ", date=" + date + ", time=" + time + ", hz=" + hz + ", disp=" + disp + ", flo=" + flo + ", sedPPM=" + sedPPM + ", psi=" + psi + ", chlPPM=" + chlPPM + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Sensor other = (Sensor) obj;
        if (!Objects.equals(this.resid, other.resid)) {
            return false;
        }
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        if (!Objects.equals(this.time, other.time)) {
            return false;
        }
        if (!Objects.equals(this.hz, other.hz)) {
            return false;
        }
        if (!Objects.equals(this.disp, other.disp)) {
            return false;
        }
        if (!Objects.equals(this.flo, other.flo)) {
            return false;
        }
        if (!Objects.equals(this.sedPPM, other.sedPPM)) {
            return false;
        }
        if (!Objects.equals(this.psi, other.psi)) {
            return false;
        }
        if (!Objects.equals(this.chlPPM, other.chlPPM)) {
            return false;
        }
        return true;
    }

}
