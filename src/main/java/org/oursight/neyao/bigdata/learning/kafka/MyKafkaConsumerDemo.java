package org.oursight.neyao.bigdata.learning.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 注意：这个 group.id 不是随意填的，而是在<KAFKA>/config/consumer.properties中配置的
 * Created by neyao on 2017/4/6.
 */
public class MyKafkaConsumerDemo {

    public static void main(String[] args) throws InterruptedException {
        autoOffset();
//        manualOffset();
    }
    public static void manualOffset() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:30021");
        props.put("group.id", "test-consumer-false");
        props.put("enable.auto.commit", "fasle");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("mytest-topic-2"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            System.out.println("MyKafkaConsumerDemo.manualOffset, about to find records...");
            ConsumerRecords<String, String> records = consumer.poll(1);
            System.out.println("records: " + records);

            for (ConsumerRecord<String, String> record : records) {
//                buffer.add(record);
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                Thread.sleep(1000L);
            }
//            if (buffer.size() >= minBatchSize) {
//                insertIntoDb(buffer);
//                consumer.commitSync();
//                buffer.clear();
//            }
        }
    }

    public static void autoOffset() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.200:30021");
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("foo", "bar"));
        consumer.subscribe(Arrays.asList("mytest-topic-2"));

        while (true) {
            System.out.println("about to find records...");
            ConsumerRecords<String, String> records = consumer.poll(1);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            Thread.sleep(1000L);
        }
    }
}
