package com.zqykj.tldw.service;

import com.zqykj.tldw.common.TldwConfig;
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
        props.put("bootstrap.servers", TldwConfig.config.getString("kafka.bootstrap.servers"));
        props.put("acks", "-1");
        props.put("batch.size", 16384);
        //props.put("transactional.id", "td1");
        props.put("retries", 1);
        props.put("client.id", "foshanClient");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<String, T>(props);
        this.topic = topic;
    }

    public KafkaProducer<String, T> getProducer() {
        return this.producer;
    }

    public void send(T t) {
        final ProducerRecord<String, T> record = new ProducerRecord<String, T>(this.topic, t);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                logger.debug("key=" + record.key() + " ,value=" + record.value() + "size=" + record.value().toString()
                        .getBytes().length + " ,partition " + metadata.partition() + ", offset: " + metadata.offset());
            }
        });
    }
}
