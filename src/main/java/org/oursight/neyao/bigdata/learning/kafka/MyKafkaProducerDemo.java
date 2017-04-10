package org.oursight.neyao.bigdata.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by neyao on 2017/4/10.
 */
public class MyKafkaProducerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:30021");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("MyKafkaProducerDemo.main, about to send data from producer...");
        String testDataPrefix = "test-data-a-";
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("mytest-topic-2", testDataPrefix + Integer.toString(i), testDataPrefix + Integer.toString(i));
            producer.send(record);
            System.out.println(record + " sent");
        }
        producer.close();
    }
}
