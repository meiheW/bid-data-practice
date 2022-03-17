package com.tomster.kafka.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author meihewang
 * @date 2022/03/17  10:29
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

    private volatile long success = 0;

    private volatile long fail = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String newValue = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(), newValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success++;
        } else {
            fail++;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
