package com.zqykj.tldw.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author feng.wei
 * @date 2018/8/15
 */
public class ConsumerProperty {


    public static Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TldwConfig.config.getString("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, TldwConfig.config.getString("kafka.key.deserializer"));
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TldwConfig.config.getString("kafka.value.deserializer"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TldwConfig.config.getString("kafka.consume.group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, TldwConfig.config.getString("kafka.enable.auto.commit"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, TldwConfig.config.getString("kafka.offset.reset"));
        properties
                .put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, TldwConfig.config.getInt("kafka.max.partition.fetch.bytes"));
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, TldwConfig.config.getInt("kafka.max.poll.records"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");

        return properties;
    }

    public static Properties consumerBackupProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TldwConfig.config.getString("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, TldwConfig.config.getString("kafka.key.deserializer"));
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TldwConfig.config.getString("kafka.value.deserializer"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TldwConfig.config.getString("kafka.consume.backup.group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, TldwConfig.config.getString("kafka.enable.auto.commit"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, TldwConfig.config.getString("kafka.offset.reset"));
        properties
                .put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, TldwConfig.config.getInt("kafka.max.partition.fetch.bytes"));
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, TldwConfig.config.getInt("kafka.max.poll.records"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");

        return properties;
    }

}
