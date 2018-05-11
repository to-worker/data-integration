package com.zqykj.tldw.service;

import com.alibaba.fastjson.JSON;
import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.hyjj.entity.elp.ElpModelDBMapping;
import com.zqykj.hyjj.entity.elp.Entity;
import com.zqykj.hyjj.entity.elp.Link;
import com.zqykj.tldw.bussiness.ElpTransformer;
import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.solr.SolrClient;
import com.zqykj.tldw.util.BeanUtils;
import com.zqykj.tldw.util.ObjAnalysis;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

import static com.zqykj.tldw.common.ElpDBMappingCache.*;

/**
 * @author alfer
 */
public class DataConsumer implements Runnable {

    private static Logger dataLogger = LoggerFactory.getLogger(DataConsumer.class);

    String consumerName;
    String topic;
    KafkaConsumer<String, byte[]> consumer;
    int partition = -1;
    TopicPartition topicPartition;
    List<TopicPartition> topicPartitions;
    Long fromOffsets;
    Long toOffsets;

    String zkHost;
    SolrClient relationSolrClient = null;
    SolrClient entitySolrClient = null;

    public DataConsumer(Properties properties, int partition, String topic, String zkHost) {
        this.consumerName = "DataConsumer-" + partition;
        dataLogger.info("partition: {}, topic: {}, zkHost:{}", partition, topic, zkHost);
        this.topic = topic;
        this.partition = partition;
        consumer = new KafkaConsumer<String, byte[]>(properties);
        topicPartition = new TopicPartition(topic, partition);
        topicPartitions = new ArrayList<>();
        topicPartitions.add(topicPartition);

        fromOffsets = consumer.beginningOffsets(topicPartitions).get(topicPartition);
        toOffsets = consumer.endOffsets(topicPartitions).get(topicPartition);

        consumer.assign(Arrays.asList(topicPartition));

        this.zkHost = zkHost;
        relationSolrClient = new SolrClient(zkHost, Constants.SOLR_RELATION_COLLECTION);
        entitySolrClient = new SolrClient(zkHost, Constants.SOLR_ENTITY_COLLECTION);
    }

    @Override
    public void run() {
        Link bayonetLink = ELP_MODEL_LINK_PROPERTY.get(Constants.LINK_BAYONET_PASS_RECORD);
        Entity vehicleEntity = ELP_MODEL_ENTITY_PROPERTY.get(Constants.ENTITY_VEHICLE);
        dataLogger.info("bayonetLink: {}", JSON.toJSONString(bayonetLink));
        dataLogger.info("vehicleEntity: {}", JSON.toJSONString(vehicleEntity));

        while (true) {
            try {
                List<Map<String, Object>> bayonetRecordList = new ArrayList<>();
                List<Map<String, Object>> vehicleList = new ArrayList<>();
                // fetch data from kafka
                ConsumerRecords<String, byte[]> records = consumer.poll(Constants.ZK_TIME_OUT_DEFAULT);
                List<ConsumerRecord<String, byte[]>> recordList = records.records(topicPartition);
                if (recordList.size() > 0) {
                    fromOffsets = recordList.get(0).offset();
                    toOffsets = recordList.get(recordList.size() - 1).offset();

                    for (ConsumerRecord<String, byte[]> record : recordList) {
                        ProviderVehicleInfo vehicleInfo = (ProviderVehicleInfo) BeanUtils.toObject(record.value());
                        dataLogger.debug("卡口ID: {}", vehicleInfo.getKkbh());
                        dataLogger.debug("车道ID: {}, 车道方向: {}", vehicleInfo.getCdbh(), vehicleInfo.getCdfx());
                        dataLogger.debug("车辆编号: {}, 车辆速度: {}", vehicleInfo.getCdbh(), vehicleInfo.getClsd());

                        Map<String, Object> beanMap = ObjAnalysis.convertObjToMap(vehicleInfo);
                        beanMap.put("hphmId", vehicleInfo.getHphm());

                        bayonetRecordList.add(getColMapValue(beanMap, BAYONET_ELPTYPE_COLUMN_MAP, BAYONET_COLUMNS));
                        vehicleList.add(getColMapValue(beanMap, VEHICLE_ELPTYPE_COLUMN_MAP, VEHICLE_COLUMNS));
                    }
                }
                //  1、elp trans； 2、persist to solr
                persistSolr(bayonetRecordList, bayonetLink,
                        ELPMODEL_DBMAPPINGS.get(Constants.LINK_BAYONET_PASS_RECORD));
                persistSolr(vehicleList, vehicleEntity);
                Thread.sleep(10000);
            } catch (Exception e) {
                dataLogger.error("occur to exception when consume data: {}", e);
            } finally {
                dataLogger.info("topic: {}, partition: {}, offsets from {} to {}.", topicPartition.topic(),
                        topicPartition.partition(), fromOffsets, toOffsets);
                consumer.commitAsync();
            }

        }
    }

    public Map<String, Object> getColMapValue(Map<String, Object> beanMap, Map<String, String> columnTypeMap,
            List<String> columnList) {
        Map<String, Object> recordMap = new HashMap<>();
        for (String column : columnList) {
            recordMap.put(columnTypeMap.get(column), beanMap.get(column));
        }
        return recordMap;
    }

    /**
     * convert elpdata to SolrInputDocumnt and persist to solr.
     *
     * @param listMap
     * @param element
     */
    public void persistSolr(List<Map<String, Object>> listMap, Link element, ElpModelDBMapping mapping) {
        dataLogger.info("{} of consumer writes {} records to {} relatoin to solr.", "DataConsumer-" + this.partition,
                listMap.size(), element.getUuid());
        if (CollectionUtils.isNotEmpty(listMap)) {
            List<SolrInputDocument> solrDocs = new ArrayList<>();
            for (Map<String, Object> bayonetRecord : listMap) {
                SolrInputDocument solrInputDocument = ElpTransformer
                        .parseLink(bayonetRecord, ELP_MODEL, element, mapping);
                if (null != solrInputDocument) {
                    solrInputDocument.setField("_indexed_at_tdt", new Date());
                    solrDocs.add(solrInputDocument);
                }
                if (solrDocs.size() > 10000) {
                    relationSolrClient.sendBatchToSolr(solrDocs);
                }
            }
            if (solrDocs.size() > 0) {
                relationSolrClient.sendBatchToSolr(solrDocs);
            }
        }

    }

    public void persistSolr(List<Map<String, Object>> listMap, Entity element) {
        dataLogger.info("{} of consumer writes {} records to {} entity to solr.", "DataConsumer-" + this.partition,
                listMap.size(), element.getUuid());
        if (CollectionUtils.isNotEmpty(listMap)) {
            List<SolrInputDocument> solrDocs = new ArrayList<>();
            for (Map<String, Object> record : listMap) {
                SolrInputDocument solrInputDocument = ElpTransformer.parseEntity(record, ELP_MODEL, element);
                if (null != solrInputDocument) {
                    solrInputDocument.setField("_indexed_at_tdt", new Date());
                    solrDocs.add(solrInputDocument);
                }
                if (solrDocs.size() > 10000) {
                    entitySolrClient.sendBatchToSolr(solrDocs);
                }
            }
            if (solrDocs.size() > 0) {
                entitySolrClient.sendBatchToSolr(solrDocs);
            }
        }

    }

}