package com.zqykj.tldw.common;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feng.wei
 * @date 2018/5/14
 */
public class TldwConfig {

    private static Logger logger = LoggerFactory.getLogger(TldwConfig.class);

    /**
     * 任务的固定配置参数
     */
    public static Configuration config = null;

    static {
        try {
            config = new PropertiesConfiguration(Constants.CONFIG_JOB_KAFKA_PROPERTIES);
        } catch (ConfigurationException e) {
            logger.error("get properties has exception from {}: {}", Constants.CONFIG_JOB_KAFKA_PROPERTIES, e);
        }
    }

}
