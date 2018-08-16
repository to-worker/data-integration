package com.zqykj.tldw.backup.service;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.TldwConfig;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileFilter;

/**
 * 将本地文件移到HDFS上
 */
@Component
public class MoveDataScheduler {
    private static final Logger logger = LoggerFactory.getLogger(MoveDataScheduler.class);

    /**
     * hdfs文件服务
     */
    @Autowired
    private HdfsService hdfsService;
    
    /**
     * 读取目录
     */
    private String readPath;
    
    /**
     * 移动到hdfs相对目录
     */
    private String movePath;
    
    @PostConstruct
    public void init() throws ConfigurationException {
        readPath = TldwConfig.config.getString(Constants.BACKUP_READ_PATH);
        movePath = TldwConfig.config.getString(Constants.BACKUP_MOVE_PATH);
    }

    @Scheduled(cron = "0 * * * * *")
    public void moveFile() throws InterruptedException {
        File localDir = new File(readPath);
        if (localDir.listFiles() == null || localDir.listFiles().length == 0) {
            logger.debug("监测:{} 是否有需要上传到hdfs上的文件:0", localDir.getAbsolutePath());
            return;
        }

        for (File dataSchemaFile : localDir.listFiles()) {
            File[] avroFiles = dataSchemaFile.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".avro");
                }
            });
            logger.debug("监测:{} 是否有需要上传到hdfs上的文件:{}", dataSchemaFile.getAbsolutePath(), avroFiles.length);
            
            for (File file : avroFiles) {
                boolean isSuccess = hdfsService.uploadLocalFile(movePath + File.separator + dataSchemaFile.getName() + File.separator + file.getName(),
                        file.getAbsolutePath());

                if (isSuccess) {
                    // 删除原文件
                    file.delete();
                    logger.info("删除上传后的文件:{}", file.getName());
                }
            }
        }
    }
}
