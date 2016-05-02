
package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import model.Sensor;

public class SensorDAO {

    public static final String userdirectory = ".";
    public static final String tableName = userdirectory + "/sensor";
    public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);

    // Column Family is 'alert'
    public static final byte[] cfAlertBytes = Bytes.toBytes("alert");

    public static final byte[] colHzBytes = Bytes.toBytes("hz");
    public static final byte[] colDispBytes = Bytes.toBytes("disp");
    public static final byte[] colFloBytes = Bytes.toBytes("flo");
    public static final byte[] colSedBytes = Bytes.toBytes("sedPPM");
    public static final byte[] colPsiBytes = Bytes.toBytes("psi");
    public static final byte[] colChlBytes = Bytes.toBytes("chlPPM");

    Configuration conf = null;
    private HTableInterface table = null;
    private static final Logger log = Logger.getLogger(SensorDAO.class);

    public SensorDAO(HTableInterface tableInterface) {
        this.table = tableInterface;
    }

    public SensorDAO(Configuration conf) throws IOException {
        this.conf = conf;
        this.table = new HTable(conf, TABLE_NAME);
    }

    public Get mkGet(String stock) throws IOException {
        log.debug(String.format("Creating Get for %s", stock));
        Get g = new Get(Bytes.toBytes(stock));
        g.addFamily(cfAlertBytes);
        System.out.println("mkGet   [rowkey= " + stock + "]");
        return g;
    }



    private Scan mkScan() {
        Scan s = new Scan();
        s.addFamily(cfAlertBytes);
        System.out.println("mkScan for Sensor table ");
        return s;
    }

    public void putSensorList(List<Put> puts) throws IOException {
        table.put(puts);
    }

    public void putSensorBatch(List<Put> puts) throws Exception {
        Object[] results = new Object[puts.size()];
        table.batch(puts, results);
        for (Object result : results) {
            System.out.println("batch put result: " + result);
        }
    }

    public Sensor getSensor(String stockId) throws IOException {
        Result result = getSensorRow(stockId);
        // create Sensor Model Object from Get Result
        Sensor inv = createSensor(result);
        // close Htable interface
        return inv;
    }

    public Result getSensorRow(String stockId) throws IOException {

        // make a get object
        Get g = mkGet(stockId);
        // call htable.get with get object
        Result result = table.get(g);
        if (result.isEmpty()) {
            log.info(String.format("Sensor %s not found.", stockId));
            return null;
        }
        // System.out.println("Get Sensor Result :");
        // System.out.println(resultToString(result));

        return result;
    }

    public List<Sensor> getSensors(int numb) throws IOException {
        int count = 0;
        // make a scan object
        Scan scan = mkScan();
        // call htable.getScanner with scan object
        ResultScanner results = table.getScanner(scan);
        // create a list of Sensor objects to return
        ArrayList<Sensor> inventoryList = new ArrayList<Sensor>();
        System.out.println("Scan Sensor Results :");
        for (Result result : results) {
            System.out.println(Tools.resultMapToString(result));
            // create Sensor Model Object from Result
            Sensor inv = createSensor(result);
            // add inventory object to list
            inventoryList.add(inv);
            count++;
            if (count>=numb) break;
        }

        // return list of inventory objects
        return inventoryList;
    }
    public Put mkPut(Sensor sensor) {
        String dateTime = sensor.date + " " + sensor.time;
        // create a composite row key: sensorid_date time
        String key = sensor.resid + "_" + dateTime;
        Put put = new Put(Bytes.toBytes(key)); //
        put.addColumn(cfAlertBytes, colHzBytes, Bytes.toBytes(sensor.hz));
        put.addColumn(cfAlertBytes, colDispBytes, Bytes.toBytes(sensor.disp));
        put.addColumn(cfAlertBytes, colFloBytes, Bytes.toBytes(sensor.flo));
        put.addColumn(cfAlertBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM));
        put.addColumn(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi));
        put.addColumn(cfAlertBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM));
        System.out.println("mkPut   [" + sensor + "]");
        return put;
    }
    /*
     * create Sensor Model Object from Get or Scan Result
     */
    public Sensor createSensor(Result result) {
        // call Sensor constructor with row key and quantity

        String rowkey = Bytes.toString(result.getRow());
        String p0 = rowkey;
        Double p1 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("hz")));
        Double  p2 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("disp")));
        Double  p3 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("flo")));
        Double  p4 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("sedPPM")));
        Double  p5 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("psi")));
        Double  p6 = Bytes.toDouble(result.getValue(cfAlertBytes, Bytes.toBytes("chlPPM")));
        Sensor inv = new Sensor(p0, p1, p2, p3, p4, p5, p6);
        return inv;
    }



    public static String resultToString(byte[] row, byte[] family,
            byte[] qualifier, byte[] value) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("Result with rowKey " + Bytes.toString(row) + " : ");
        strBuilder.append(" Family - " + Bytes.toString(family));
        strBuilder.append(" : Qualifier - " + Bytes.toString(qualifier));
        strBuilder.append(" : Value: " + Bytes.toLong(value));
        return strBuilder.toString();
    }

    public void close() throws IOException {
        table.close();
    }
}
