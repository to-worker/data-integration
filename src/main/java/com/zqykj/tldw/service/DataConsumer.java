package com.zqykj.tldw.service;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.solr.SolrClient;
import com.zqykj.tldw.util.BeanUtils;
import com.zqykj.tldw.util.ObjAnalysis;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.zqykj.tldw.common.ElpDBMappingCache.*;

/**
 * @author alfer
 */
public class DataConsumer implements Runnable {

    private static Logger dataLogger = LoggerFactory.getLogger(DataConsumer.class);

    String consumerName;
    KafkaConsumer<String, byte[]> consumer;
    int partition = -1;
    String zkHost;
    String collectionName;
    SolrClient solrClient;

    public DataConsumer(Properties properties, int partition, String topic, String zkHost, String collectionName) {
        this.consumerName = "DataConsumer-" + partition;
        this.partition = partition;
        consumer = new KafkaConsumer<String, byte[]>(properties);
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
                Map<String, List<Map<String, Object>>> mapLists = new HashMap<String, List<Map<String, Object>>>();
                List<Map<String, Object>> bayonetRecordList = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> vehicleList = new ArrayList<Map<String, Object>>();
                // fetch data from kafka
                ConsumerRecords<String, byte[]> records = consumer.poll(Constants.ZK_TIME_OUT_DEFAULT);
                for (ConsumerRecord<String, byte[]> record : records) {
                    ProviderVehicleInfo vehicleInfo = (ProviderVehicleInfo) BeanUtils.toObject(record.value());
                    dataLogger.debug("卡口ID: {}", vehicleInfo.getKkbh());
                    dataLogger.debug("车道ID: {},车道方向: {}", vehicleInfo.getCdbh(), vehicleInfo.getCdfx());
                    dataLogger.debug("车辆编号: {}, 车辆速度: {}", vehicleInfo.getCdbh(), vehicleInfo.getClsd());

                    Map<String, Object> beanMap = ObjAnalysis.convertObjToMap(vehicleInfo);

                    bayonetRecordList.add(getColMapValue(beanMap, BAYONET_ELPTYPE_COLUMN_MAP, BAYONET_COLUMNS));
                    vehicleList.add(getColMapValue(beanMap, VEHICLE_ELPTYPE_COLUMN_MAP, VEHICLE_COLUMNS));
                }
                consumer.commitAsync();
                // TODO elp trans
                // TODO save to solr
            } catch (Exception e) {
                dataLogger.error("occur to exception when consume data: {}", e.getStackTrace());
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
}