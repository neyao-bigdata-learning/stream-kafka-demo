package org.oursight.neyao.bigdata.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by neyao on 2017/4/10.
 */
public class MyKafkaProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:36053");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("MyKafkaProducerDemo.main, about to send data from producer...");
        String testDataPrefix = "test-data-a-";
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("mytest-topic-2", testDataPrefix + Integer.toString(i), testDataPrefix + Integer.toString(i));
            Future<RecordMetadata> future = producer.send(record);
//            Future<RecordMetadata> future = producer..send(record);
            System.out.println(record + ", sent, result: " + future.get());
        }
        producer.close();
    }
}
