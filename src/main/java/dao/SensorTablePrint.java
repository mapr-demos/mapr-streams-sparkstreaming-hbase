package dao;

import java.io.IOException;
import java.util.List;
import model.Sensor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;

public class SensorTablePrint {

    /**
     * This objective of this lab exercise is to: 1) Save data to the HBase
     * tables using put operation 2) Retrieve and print the data from the HBase
     * tables using get & scan operations and Result object. 3) Use Put List to
     * batch them and also use write buffer for single puts. 4) remove rows and
     * columns from a Table.
     *
     * @author Sridhar Reddy
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        SensorDAO sensorDAO = new SensorDAO(conf);

        printTable(sensorDAO);

    }

    public static void printTable(SensorDAO dao) throws IOException {
        System.out.println("print 20 rows from Table");
        List<Sensor> list = dao.getSensors(20);
        System.out
                .println("*****************************************************");
        System.out.println("print Sensors from Table Column family data ...");
        for (Sensor sensor : list) {
            System.out.println(sensor);
        }
        System.out
                .println("*****************************************************");
        System.out.println("print 20 rows from Table Column family alerts");
        dao.printAlerts(20);

        System.out
                .println("*****************************************************");
        System.out.println("print 20 rows from Table Column family stats");
        dao.printStats(20);
    }

}
