/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;

import dao.SensorDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import model.Sensor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;

public class MyConsumerWriteHBase {

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        SensorDAO dao = new SensorDAO(conf);
        configureConsumer(args);

        String topic = "/user/user01/pump:sensoralert";
        if (args.length == 1) {
            topic = args[0];
        }

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;
        long waitTime = 300 * 1000;  // loop for while loop  
        long numberOfMsgsReceived = 0;
        List<Put> puts = null;
        Sensor sensor;
        while (waitTime > 0) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> msg = consumer.poll(pollTimeOut);
            if (msg.count() == 0) {
                System.out.println("No messages after 1 second wait.");
            } else {
                System.out.println("Read " + msg.count() + " messages");
                numberOfMsgsReceived += msg.count();
                System.out.println(" number of messages received total" + numberOfMsgsReceived);
                puts = new ArrayList<>();
                // Iterate through returned records, extract the value
                // of each message, and print the value to standard output.
                Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<String, String> record = iter.next();
                    System.out.println("Consuming " + record.toString());
                    String str = record.value();
                    sensor = new Sensor(str);
                    puts.add(dao.mkPut(sensor));
                }
                System.out.println(" write to hbase" + puts);
                dao.putSensorList(puts);
            }
            waitTime = waitTime - 1000; // decrease time for loop
        }
        consumer.close();
    }

    /* Set the value for configuration parameters.*/
    public static void configureConsumer(String[] args) {
        Properties props = new Properties();
        // cause consumers to start at beginning of topic on first read
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

}
