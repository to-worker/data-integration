package com.zqykj.tldw.tool.kafka;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.common.TldwConfig;
import com.zqykj.tldw.service.Producer;
import com.zqykj.tldw.tool.service.BayonetRecordService;
import com.zqykj.tldw.util.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author feng.wei
 * @date 2018/5/16
 */
public class PersudeProducer {

    private static Logger logger = LoggerFactory.getLogger(PersudeProducer.class);

    public static void main(String[] args) {
        String topicName = TldwConfig.config.getString("kafka.topic.name");
        logger.info("begin persude data to topic: {} of kafka.", topicName);
        BayonetRecordService bayonetRecordService = new BayonetRecordService();
        Producer<byte[]> producer = null;
        List<ProviderVehicleInfo> vehicleInfos = bayonetRecordService.getAll();
        logger.info("get {} size and prepare send to kafka.", vehicleInfos.size());
        if (vehicleInfos.size() > 0) {
            producer = new Producer<byte[]>(topicName);
            for (ProviderVehicleInfo info : vehicleInfos) {
                producer.send(BeanUtils.toByteArray(info));
            }
            producer.getProducer().flush();
            producer.getProducer().close();
        }
    }

}
