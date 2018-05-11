package com.zqykj.tldw.common;

/**
 * @author feng.wei
 * @date 2018/5/8
 */
public interface Constants {

    /**
     * kafka配置文件
     */
    String CONFIG_JOB_KAFKA_PROPERTIES = "./config/job/job.properties";

    /**
     * 车辆通行记录标识
     */
    String LINK_BAYONET_PASS_RECORD = "bayonet_pass_record";

    /**
     * 车辆标识
     */
    String ENTITY_VEHICLE = "vehicle";

    Integer ZK_TIME_OUT_DEFAULT = 30000;

    /**
     * solr实体collection
     */
    String SOLR_ENTITY_COLLECTION = "global_foshan_standard_model_entity_index";

    /**
     * solr连接collection
     */
    String SOLR_RELATION_COLLECTION = "global_foshan_standard_model_relation_index";

}
