package com.zqykj.tldw.common;

/**
 * @author feng.wei
 * @date 2018/5/8
 */
public interface Constants {

    /**
     * kafka配置文件
     */
    String CONFIG_JOB_KAFKA_PROPERTIES = "./config/job/kafka.properties";

    /**
     * 车辆通行记录标识
     */
    String LINK_BAYONET_PASS_RECORD = "bayonet_pass_record";

    /**
     * 车辆标识
     */
    String ENTITY_VEHICLE = "vehicle";

    Integer ZK_TIME_OUT_DEFAULT = 30000;

}
