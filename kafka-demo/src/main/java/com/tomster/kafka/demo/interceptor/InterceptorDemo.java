package com.tomster.kafka.demo.interceptor;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author meihewang
 * @date 2022/03/17  9:33
 */
public class InterceptorDemo {

    private static String TOPIC = "tomster.demo.topic";

    private static String BROKER_LIST = "localhost:9092";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers", BROKER_LIST);
        prop.put("group.id", "consumer.demo.id");
        //配置消费者拦截器
        prop.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());
        //...
    }

    public static void produce(String[] args) {
        Properties prop = new Properties();
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers", BROKER_LIST);
        //配置消费者拦截器
        prop.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        //...
    }
}
