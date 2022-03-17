package com.tomster.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author meihewang
 * @date 2022/03/14  11:12
 */
public class BasicDemo {

    private static String TOPIC = "tomster.demo.topic";

    private static String BROKER_LIST = "localhost:9092";

    public void produceMsg(String msg) {
        Properties prop = new Properties();
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers", BROKER_LIST);
        //生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        //构建消息
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello kafka!!!");
        //record.key();
        //发送消息
        producer.send(record);
        producer.close();

    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers", BROKER_LIST);
        prop.put("group.id", "consumer.demo.id");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singletonList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                //record.offset();
                //record.partition();
                //record.key();
                System.out.println(record.value());
            }

        }
    }
}
