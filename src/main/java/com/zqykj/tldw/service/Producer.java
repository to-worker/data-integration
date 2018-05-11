package com.zqykj.tldw.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by fengwei on 17/5/15.
 */
public class Producer<T> {

    private static Logger logger = LoggerFactory.getLogger(Producer.class);
    private KafkaProducer<String, T> producer = null;
    private String topic;

    public Producer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "zqykjdev14:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<String, T>(props);
        this.topic = topic;
    }

    public void send(T t) {
        final ProducerRecord<String, T> record = new ProducerRecord<String, T>(this.topic, t);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
//                logger.debug("time=" + record.timestamp() + " ,key=" + record.key() + " ,value=" + record.value()
//                        + " ,partition " + metadata.partition() + ", offset: " + metadata.offset());
            }
        });
    }
}
