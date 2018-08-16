package com.zqykj.tldw.backup.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.hyjj.query.tldw.TLDWCondition;
import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.ConsumerProperty;
import com.zqykj.tldw.common.TldwConfig;
import com.zqykj.tldw.util.BeanUtils;
import com.zqykj.tldw.util.DateUtils;
import com.zqykj.tldw.util.SchemaUtil;
import com.zqykj.tldw.util.UnitUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author feng.wei
 * @date 2018/8/15
 */
@Component("backupService")
public class BackupService {

    private static Logger logger = LoggerFactory.getLogger(BackupService.class);

    private static String unitRegex = "(\\d+)([GgMmKk])?";

    private Schema schema = null;
    private String tableName;
    private String topicName;

    /**
     * 线程执行器,每个partition一个线程
     */
    private ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * 读取目录
     */
    private String readPath;

    /**
     * 缓存数据超时时间
     */
    private long dataBufferOverTime;

    private long maxSize;

    /**
     * 单个dataSchema缓存的最大值
     */
    private String maxSizeString;

    public void init() throws Exception {
        topicName = TldwConfig.config.getString(Constants.CONFIG_KAFKA_TOPIC);
        readPath = TldwConfig.config.getString(Constants.BACKUP_READ_PATH);
        dataBufferOverTime = TldwConfig.config
                .getLong(Constants.BACKUP_READ_MAX_TIME, Constants.BACKUP_READ_MAX_TIME_DEFAULT);

        maxSizeString = TldwConfig.config.getString(Constants.BACKUP_READ_MAX_SIZE);
        maxSize = UnitUtil.getSizeByUnit(maxSizeString);

        Map<String, String> columnMapType = BeanUtils.getFields(new ProviderVehicleInfo());
        Map<String, String> avroNameTypeMap = SchemaUtil.getAvroNameTypeMap(columnMapType);
        schema = SchemaUtil.getSchema(avroNameTypeMap, this.tableName);

        Properties properties = ConsumerProperty.consumerBackupProperties();
        KafkaConsumer consumer = new KafkaConsumer<String, byte[]>(properties);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topicName);
        int partitions = partitionsFor.size();
        logger.info("topic:{} has {} partitions.", topicName, partitions);
        for (int i = 0; i < partitions; i++) {
            ReadConsumer readConsumer = new ReadConsumer(properties, i,
                    TldwConfig.config.getString(Constants.CONFIG_KAFKA_TOPIC));

            executorService.execute(readConsumer);
        }
    }

    class ReadConsumer implements Runnable {

        String consumerName;
        KafkaConsumer<String, byte[]> consumer;
        TopicPartition topicPartition;
        int partition = -1;
        Long fromOffsets;
        Long toOffsets;

        public ReadConsumer(Properties props, int partition, String topicName) {
            this.consumerName = "readCoumer-" + partition;
            this.partition = partition;
            consumer = new KafkaConsumer<String, byte[]>(props);
            topicPartition = new TopicPartition(topicName, partition);
            consumer.assign(Arrays.asList(topicPartition));
        }

        @Override
        public void run() {
            logger.info("{} 启动,读取数据", consumerName);
            // 数据缓存容器
            DataBufferContainer dataBufferContainer = new DataBufferContainer();
            BufferedWriter bw = null;

            while (true) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Constants.ZK_TIME_OUT_DEFAULT);
                    if (records != null && records.count() > 0) {
                        List<ConsumerRecord<String, byte[]>> recordList = records.records(topicPartition);
                        fromOffsets = recordList.get(0).offset();
                        toOffsets = recordList.get(recordList.size() - 1).offset();
                        logger.info("topic:{}, partition:{}, offsets from {} to {}", topicName, partition, fromOffsets, toOffsets);
                        ProviderVehicleInfo vehicleInfo;
                        for (ConsumerRecord<String, byte[]> record : records) {

                            vehicleInfo = (ProviderVehicleInfo) BeanUtils.toObject(record.value());
                            String vStr = JSON.toJSONString(vehicleInfo);
                            if (dataBufferContainer.totalSize == 0) {
                                logger.debug("读取第一条记录:partition:{}, offset:{}, message:{}", record.partition(),
                                        record.offset(), vStr);
                            }

                            // 获取dataSchemaId对应的数据缓存
                            dataBufferContainer.addMessage(vStr, record.offset(), record.partition());

                            // 判断数据是否写入文件
                            if (dataBufferContainer.totalSize > maxSize) {
                                saveDataBufferMap(dataBufferContainer, this);
                            }
                        }
                    } else {
                        long currentTime = System.currentTimeMillis();
                        logger.info("{} 没有数据更新, 等待读取下一轮数据", consumerName);
                        if (dataBufferContainer.saveTime != -1 && currentTime > dataBufferContainer.saveTime) {
                            logger.info("缓存数据超时,落地存储:记录大小:{}KB", dataBufferContainer.totalSize * 1.0 / 1024);
                            saveDataBufferMap(dataBufferContainer, this);
                        }
                    }
                    Thread.sleep(TldwConfig.config.getLong("kafka.fetch.interval.millisecond", 4000));
                } catch (Exception e) {
                    logger.error("读取kafka数据,备份到本地失败:{}", e);
                } finally {
                    if (bw != null) {
                        try {
                            bw.close();
                        } catch (IOException e) {
                            logger.error("关闭文件流失败:{}", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 保存缓存数据到文件
     *
     * @param dataBufferContainer
     * @param consumer
     * @throws Exception
     */
    private synchronized void saveDataBufferMap(DataBufferContainer dataBufferContainer, ReadConsumer consumer)
            throws Exception {
        // 无数据缓存,则不清除
        if (dataBufferContainer.totalSize == 0) {
            return;
        }

        Map<String, String> tempFileMap = new HashMap<String, String>();
        try {
            DataBuffer dataBuffer = dataBufferContainer.dataBuffer;

            // 缓存无数据时,则过滤
            if (dataBuffer.size > 0) {
                String filePath =
                        readPath + File.separator + DateUtils.getDate() + File.separator + consumer.consumerName
                                + "-" + System.currentTimeMillis()
                                + Constants.FILE_NAME_AVRO;
                String tempFilePath = filePath + Constants.FILE_NAME_TEMP;

                tempFileMap.put(tempFilePath, filePath);

                logger.info("缓存数据大小:{}KB, 写入文件:{}", dataBuffer.size * 1.0 / 1024, tempFilePath);

                AvroWriter writer = new AvroWriter();
                boolean isSuccess = writer.writer(dataBuffer, tempFilePath);
                // 成功则清除数据,若是写入本地文件一直失败,当数据量超过10倍限制时,强制清除缓存
                if (isSuccess || dataBuffer.size > maxSize * 10) {
                    dataBuffer.clear();
                } else {
                    throw new Exception("写入缓存数据到本地文件失败:" + tempFilePath);
                }
            }

            // 文件重命名
            for (Map.Entry<String, String> entry : tempFileMap.entrySet()) {
                File tempFile = new File(entry.getKey());
                File avroFile = new File(entry.getValue());
                boolean isSuccess = tempFile.renameTo(avroFile);
                if (!isSuccess) {
                    throw new Exception("重命名文件:" + tempFile.getName() + " to " + avroFile.getName() + "出错");
                }
            }

            // 存储offset// 存储offsetmp
            logger.info("存储partition:{}, offset:{}", consumer.partition, dataBufferContainer.minOffsetMap);
            consumer.consumer.commitSync();

            // 清除缓存
            dataBufferContainer.clear();
        } catch (Exception e) {
            logger.info("写入缓存数据失败:{}", e);

            // 只要是落地数据出错,则删除全部落地文件
            for (Map.Entry<String, String> entry : tempFileMap.entrySet()) {
                File tempFile = new File(entry.getKey());
                File avroFile = new File(entry.getValue());
                if (tempFile.exists()) {
                    tempFile.delete();
                }
                if (tempFile.exists()) {
                    avroFile.delete();
                }
            }

            throw new Exception("数据落地出错");
        }
    }

    class AvroWriter {
        boolean writer(DataBuffer dataBuffer, String filePath) throws Exception {
            DataFileWriter<GenericRecord> writer = null;
            try {
                writer = getDataFileWriter(schema, filePath);

                for (GenericRecord record : dataBuffer.list) {
                    writer.append(record);
                }
            } catch (Exception e) {
                logger.error("写入文件到avro出错:{}", e);

                File tempFile = new File(filePath);
                if (tempFile.exists()) {
                    tempFile.delete();
                }

                return false;
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }

            return true;
        }
    }

    public DataFileWriter<GenericRecord> getDataFileWriter(Schema schema, String filePath) throws Exception {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter);

        // 检测目录文件是否存在
        File avroFile = new File(filePath);
        File dir = avroFile.getParentFile();
        if (!dir.exists()) {
            boolean isSuccess = dir.mkdirs();
            if (!isSuccess) {
                throw new Exception("create avro dir:" + dir.getAbsolutePath() + " fail");
            }
        }
        writer.create(schema, avroFile);

        return writer;
    }

    class DataBufferContainer {
        DataBuffer dataBuffer = null;
        long totalSize = 0;
        long saveTime = -1;
        Map<Integer, Long> minOffsetMap = new HashMap<Integer, Long>();
        Map<Integer, Long> maxOffsetMap = new HashMap<Integer, Long>();

        /**
         * 添加数据
         *
         * @param message 信息
         * @throws Exception 异常
         */
        public void addMessage(String message, long offset, int partition) throws Exception {
            if (dataBuffer == null) {
                dataBuffer = new DataBuffer();
            }
            dataBuffer.add(message);

            // 计算保存时间和总大小,落地的两条线
            totalSize += message.length();
            saveTime = System.currentTimeMillis() + dataBufferOverTime * 1000;

            // 维护当前缓存最小offset 和 最大offset
            Long minOffset = minOffsetMap.get(partition);
            if (minOffset == null || minOffset == 0 || minOffset > offset) {
                minOffsetMap.put(partition, offset);
            }
            Long maxOffset = maxOffsetMap.get(partition);
            if (maxOffset == null || maxOffset == 0 || maxOffset < offset) {
                maxOffsetMap.put(partition, offset);
            }
        }

        public void clear() {
            minOffsetMap = maxOffsetMap;
            maxOffsetMap = new HashMap<Integer, Long>();

            totalSize = 0;
            saveTime = -1;
        }
    }

    class DataBuffer {
        List<GenericRecord> list = new ArrayList<GenericRecord>();
        long size = 0;

        void add(String message) throws Exception {
            // 读取数据,并转为avro record
            GenericRecord record = transform(schema, message);

            list.add(record);
            size += message.length();
        }

        void clear() {
            list.clear();
            size = 0;
        }

    }

    /**
     * json数据转为avro记录
     *
     * @param schema  avroSchema
     * @param message 记录
     * @return avro记录
     */
    public GenericRecord transform(Schema schema, String message) {
        GenericRecord record = new GenericData.Record(schema);

        JSONObject json = JSONObject.parseObject(message);
        for (String key : json.keySet()) {
            String type = schema.getField(key).schema().toString();
            if (type.contains("long")) {
                record.put(key, json.getLong(key));
            } else if (type.contains("boolean") || type.contains("bool")) {
                record.put(key, json.getBoolean(key));
            } else if (type.contains("double")) {
                record.put(key, json.getDouble(key));
            } else if (type.contains("int")) {
                record.put(key, json.getInteger(key));
            } else if (type.contains("bytes")) {
                record.put(key, json.getBytes(key));
            } else {
                record.put(key, json.getString(key));
            }
        }

        return record;
    }

}
