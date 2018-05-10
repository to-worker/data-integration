package com.zqykj.tldw.service;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.ElpDBMappingCache;
import com.zqykj.tldw.solr.SolrClient;
import com.zqykj.tldw.util.BeanUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author feng.wei
 * @date 2018/5/8
 */
@Component
public class ConsumeProcessor {

    private static Logger logger = LoggerFactory.getLogger(ConsumeProcessor.class);

    /**
     * 任务的固定配置参数
     */
    private Configuration config;

    /**
     * 线程执行器,每个partition一个线程
     */
    private ExecutorService executorService = Executors.newCachedThreadPool();

    @Autowired
    private ElpDBMappingService dbMappingService;

    @PostConstruct
    public void init() throws ConfigurationException {

        config = new PropertiesConfiguration(Constants.CONFIG_JOB_KAFKA_PROPERTIES);
        initializeCache();

        Properties properties = consumerProperties();
        int partitions = config.getInt("kafka.topic.partitions");
        for (int i = 0; i < partitions; i++) {
            DataConsumer dataConsumer = new DataConsumer(properties, i, config.getString("kafka.topic.name"),
                    config.getString("zk.host"), config.getString("solr.collection.name"));
            executorService.execute(dataConsumer);
            // executorService.submit(dataConsumer);
        }
    }

    public void initializeCache() {
        synchronized (ElpDBMappingCache.class) {
            if (null == ElpDBMappingCache.BAYONET_ELPTYPE_COLUMN_MAP){
                ElpDBMappingCache.BAYONET_ELPTYPE_COLUMN_MAP = dbMappingService
                        .getElpColMap(dbMappingService.getElpModelDBMappingByElpTypeAndDs(
                                config.getString("", "kafka"),
                                config.getString("","bayonet_pass_record"),
                                config.getString("","foshan_standard_model"),
                                config.getString("","bayonet_pass_record")));

            }

            if (null == ElpDBMappingCache.VEHICLE_ELPTYPE_COLUMN_MAP){
                ElpDBMappingCache.VEHICLE_ELPTYPE_COLUMN_MAP = dbMappingService
                        .getElpColMap(dbMappingService.getElpModelDBMappingByElpTypeAndDs(
                                config.getString("", "kafka"),
                                config.getString("","bayonet_pass_record"),
                                config.getString("","foshan_standard_model"),
                                config.getString("","vehicle")));

            }
        }

    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getString("kafka.key.deserializer"));
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getString("kafka.value.deserializer"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consume.group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString("kafka.enable.auto.commit"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.offset.reset"));
        properties
                .put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.getInt("kafka.max.partition.fetch.bytes"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");

        return properties;
    }

}


