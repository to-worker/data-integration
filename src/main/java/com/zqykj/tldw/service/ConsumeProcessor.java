package com.zqykj.tldw.service;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.solr.SolrClient;
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

    @PostConstruct
    public void init() throws ConfigurationException {
        System.out.println(ConsumeProcessor.class.getName());
        config = new PropertiesConfiguration(Constants.CONFIG_JOB_KAFKA_PROPERTIES);
        Properties properties = consumerProperties();
        int partitions = config.getInt("kafka.topic.partitions");
        for (int i = 0; i < partitions; i++) {
            DataConsumer dataConsumer = new DataConsumer(properties, i, config.getString("kafka.topic.name"),
                    config.getString("zk.host"), config.getString("solr.collection.name"));
            executorService.execute(dataConsumer);
            // executorService.submit(dataConsumer);
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
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");

        return properties;
    }

    static class DataConsumer implements Runnable {

        private static Logger dataLogger = LoggerFactory.getLogger(DataConsumer.class);

        String consumerName;
        KafkaConsumer<String, String> consumer;
        int partition = -1;
        String zkHost;
        String collectionName;
        SolrClient solrClient;

        public DataConsumer(Properties properties, int partition, String topic, String zkHost, String collectionName) {
            this.consumerName = "DataConsumer-" + partition;
            this.partition = partition;
            consumer = new KafkaConsumer<String, String>(properties);
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Arrays.asList(topicPartition));

            this.zkHost = zkHost;
            this.collectionName = collectionName;
            this.solrClient = new SolrClient(zkHost, collectionName);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    // TODO consumer data
                    ConsumerRecords<String, String> records = consumer.poll(Constants.ZK_TIME_OUT_DEFAULT);
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: {}, value: {}", record.key(), record.value());
                    }

                    // TODO elp trans
                    // TODO save to solr
                    System.out.println("run...");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}


