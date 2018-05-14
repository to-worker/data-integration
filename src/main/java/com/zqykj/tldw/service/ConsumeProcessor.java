package com.zqykj.tldw.service;

import com.zqykj.hyjj.entity.elp.ElpModelDBMapping;
import com.zqykj.hyjj.entity.elp.Entity;
import com.zqykj.hyjj.entity.elp.Link;
import com.zqykj.tldw.bussiness.ElpTransformer;
import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.ElpDBMappingCache;
import com.zqykj.tldw.common.TldwConfig;
import com.zqykj.tldw.timed.CleanSolrData;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author feng.wei
 * @date 2018/5/8
 */
@Component
public class ConsumeProcessor {

    private static Logger logger = LoggerFactory.getLogger(ConsumeProcessor.class);

    Configuration config = TldwConfig.config;

    @Value("${solr.zk.host}")
    private String zkHost;

    /**
     * 线程执行器,每个partition一个线程
     */
    private ExecutorService executorService = Executors.newCachedThreadPool();

    @Autowired
    private ElpDBMappingService dbMappingService;

    @Autowired
    private ElpModelService elpModelService;

    @Autowired
    private CleanSolrData cleanSolrData;

    @PostConstruct
    public void init() throws ConfigurationException {

        initializeCache();
        ElpTransformer.init(config.getString("job.mongo.hostname",""),
                config.getInt("job.mongo.port",27017),
                config.getString("job.mongo.database",""));

        Properties properties = consumerProperties();
        int partitions = config.getInt("kafka.topic.partitions");
        for (int i = 0; i < partitions; i++) {
            DataConsumer dataConsumer = new DataConsumer(properties, i, config.getString("kafka.topic.name"),
                    zkHost);
            executorService.execute(dataConsumer);
            Runtime.getRuntime().addShutdownHook(new Thread(dataConsumer){
                @Override
                public void run() {
                    logger.warn("topic: {}, partition: {}, offsets from {} to {}.", dataConsumer.topic,
                            dataConsumer.partition, dataConsumer.fromOffsets, dataConsumer.toOffsets);
                }
            });
        }
    }

    public void initializeCache() {
        synchronized (ElpDBMappingCache.class) {
            if (null == ElpDBMappingCache.ELP_MODEL){
                ElpDBMappingCache.ELP_MODEL =
                        elpModelService.getElpModelByModelId(config.getString("","foshan_standard_model"));
            }

            if (null == ElpDBMappingCache.BAYONET_ELPTYPE_COLUMN_MAP){
                ElpModelDBMapping dbMapping = dbMappingService.getElpModelDBMappingByElpTypeAndDs(
                        config.getString("", "kafka"),
                        config.getString("","bayonet_pass_record"),
                        config.getString("","foshan_standard_model"),
                        config.getString("","bayonet_pass_record"));
                ElpDBMappingCache.BAYONET_ELPTYPE_COLUMN_MAP = dbMappingService
                        .getElpColMap(dbMapping);
                ElpDBMappingCache.ELPMODEL_DBMAPPINGS.put(Constants.LINK_BAYONET_PASS_RECORD, dbMapping);
                Link link = elpModelService.findLinkByLinkUuid(Constants.LINK_BAYONET_PASS_RECORD);
                ElpDBMappingCache.ELP_MODEL_LINK_PROPERTY.put(Constants.LINK_BAYONET_PASS_RECORD, link);

            }

            if (null == ElpDBMappingCache.VEHICLE_ELPTYPE_COLUMN_MAP){
                ElpModelDBMapping dbMapping = dbMappingService.getElpModelDBMappingByElpTypeAndDs(
                        config.getString("", "kafka"),
                        config.getString("","bayonet_pass_record"),
                        config.getString("","foshan_standard_model"),
                        config.getString("","vehicle"));
                ElpDBMappingCache.VEHICLE_ELPTYPE_COLUMN_MAP = dbMappingService
                        .getElpColMap(dbMapping);

                ElpDBMappingCache.ELPMODEL_DBMAPPINGS.put(Constants.ENTITY_VEHICLE, dbMapping);
                Entity entity = elpModelService.findEntityByEntityUuid(Constants.ENTITY_VEHICLE);
                ElpDBMappingCache.ELP_MODEL_ENTITY_PROPERTY.put(Constants.ENTITY_VEHICLE, entity);


            }

            if (null == ElpDBMappingCache.BAYONET_COLUMNS){
                ElpDBMappingCache.BAYONET_COLUMNS = new ArrayList<>();
                Iterator<String> iterator = ElpDBMappingCache.BAYONET_ELPTYPE_COLUMN_MAP.keySet().iterator();
                while (iterator.hasNext()){
                    ElpDBMappingCache.BAYONET_COLUMNS.add(iterator.next());
                }
            }

            if (null == ElpDBMappingCache.VEHICLE_COLUMNS){
                ElpDBMappingCache.VEHICLE_COLUMNS = new ArrayList<>();
                Iterator<String> iterator = ElpDBMappingCache.VEHICLE_ELPTYPE_COLUMN_MAP.keySet().iterator();
                while (iterator.hasNext()){
                    ElpDBMappingCache.VEHICLE_COLUMNS.add(iterator.next());
                }
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
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getInt("kafka.max.poll.records"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");

        return properties;
    }

}


