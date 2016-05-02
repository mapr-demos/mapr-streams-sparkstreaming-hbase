package test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import dao.SensorDAO;
import dao.SensorTablePrint;
import model.Sensor;

public class TestSensorList {

    @Before
    public void setup() throws Exception {
    }

    // TODO 1a finish code run unit test observe output on console
    @Test
    public void testCreateSaveSensorData1() throws Exception {
        System.out.println(" Test 1 ");
        HTableInterface table = MockHTable.create();
        SensorDAO dao = new SensorDAO(table);
        String temp = "NANTAHALLA,3/14/14,23:54,0,0,0,1.96,0,1.14";
        List<Put> puts = new ArrayList<Put>();

        System.out.println("------------------------------");
        System.out.println(" Inserting rows in Sensor Table: ");
        Sensor sensor = new Sensor(temp);
        puts.add(dao.mkPut(sensor));
        temp = "NANTAHALLA,3/15/14,23:54,1.0,2.0,3.0,1.96,0,1.14";
        sensor = new Sensor(temp);
        puts.add(dao.mkPut(sensor));
        dao.putSensorList(puts);
        List<Sensor> list = dao.getSensors(3);
        assertEquals(2, list.size());
        System.out.println(" Print results for Sensor Table: ");
        for (Sensor inv : list) {
            System.out.println(inv);
        }
        
        SensorTablePrint.printTable(dao);

    }

}
