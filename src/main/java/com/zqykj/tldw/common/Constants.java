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

    String QUERY_CONDITION_OPERATOR = ":";

    /**
     * kafka topic
     */
    public static final String CONFIG_KAFKA_TOPIC = "kafka.topic.name";

    /**
     * 备份:读取本地文件名
     */
    String BACKUP_READ_PATH = "backup.read.local.path";

    /**
     * 备份:读取数据缓存最大时间
     */
    String BACKUP_READ_MAX_TIME = "backup.read.max.time";

    /**
     * 备份:读取数据缓存最大值
     */
    String BACKUP_READ_MAX_SIZE = "backup.read.max.size";

    /**
     * 备份:读取缓存数据最大时间的默认值
     */
    long BACKUP_READ_MAX_TIME_DEFAULT = 60;

    /**
     * 备份:移动到hdfs路径经
     */
    String BACKUP_MOVE_PATH = "backup.move.path";

    /**
     * 备份:合并到hdfs路径
     */
    String BACKUP_MERGE_PATH = "backup.merge.path";

    /**
     * 备份:合并hdfs文件的最大值,超过该值,则合并文件
     */
    String BACKUP_MERGE_MAX_SIZE = "backup.merge.max.size";

    /**
     * utf-8编码方式
     */
    public static final String ENCODING_UTF_8 = "UTF-8";

    /**
     * 临时文件名后缀
     */
    public static final String FILE_NAME_TEMP = ".temp";

    /**
     * avro文件名后缀
     */
    public static final String FILE_NAME_AVRO = ".avro";

    /**
     * 读取avro的code值
     */
    public static final String AVRO_CODEC = "avro.codec";

}
