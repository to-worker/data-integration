package com.zqykj.tldw.backup.service;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.TldwConfig;
import com.zqykj.tldw.util.DateUtils;
import com.zqykj.tldw.util.UnitUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;

/**
 * 合并数据定时任务
 * 
 */
@Component
public class MergeDataScheduler {
    private static final Logger logger = LoggerFactory.getLogger(MergeDataScheduler.class);

    /**
     * hdfs文件服务
     */
    @Autowired
    private HdfsService hdfsService;
    
    /**
     * 合并目录
     */
    private String mergePath;
    
    /**
     * 移动到hdfs相对目录
     */
    private String movePath;
    
    /**
     * 合并文件大小上限
     */
    private String maxSizeString;
    private long maxSize;
    
    @PostConstruct
    public void init() throws Exception {
        // 读取配置参数
        movePath = TldwConfig.config.getString(Constants.BACKUP_MOVE_PATH);
        mergePath = TldwConfig.config.getString(Constants.BACKUP_MERGE_PATH);
        
        maxSizeString = TldwConfig.config.getString(Constants.BACKUP_MERGE_MAX_SIZE);
        maxSize = UnitUtil.getSizeByUnit(maxSizeString);

    }

    @Scheduled(cron = "0 * * * * *")
    public void mergeFile() {
        try {
            FileStatus[] dataSchemaFiles = hdfsService.list(movePath);
            if (dataSchemaFiles == null) {
                logger.debug("文件目录:{} 不存在, 无合并", movePath);
                return;
            }

            for (FileStatus dataSchemaFile : dataSchemaFiles) {
                Path dataSchemaPath = dataSchemaFile.getPath();
                // 获取文件列表
                FileStatus[] fileStatuses = hdfsService.list(dataSchemaPath, Constants.FILE_NAME_AVRO);
                if (fileStatuses == null) {
                    logger.debug("文件目录:{} 不存在, 无合并", dataSchemaPath.getName());
                    continue;
                }

                // 检测文件大小,并判断是否合并文件
                long filesSize = filesSize(fileStatuses);
                if (filesSize > maxSize) {
                    String dstFile = mergePath + File.separator + DateUtils.getDate() + File.separator + getCompactFileName(fileStatuses);
                    logger.info("{}下文件大小为:{}, 合并文件到:{}", dataSchemaPath.getName(), filesSize, dstFile);

                    hdfsService.mergeFiles(dataSchemaPath, dstFile, fileStatuses);
                } else {
                    logger.debug("{}下文件大小为:{}, 不进行文件合并", dataSchemaPath.getName(), filesSize);
                }
            }
        } catch (Exception e) {
            logger.error("合并文件失败:{}", e);
        }
    }

    /**
     * 计算文件列表大小
     * 
     * @param fileStatuses
     *            文件列表信息
     * @return 文件列表总大小
     */
    private long filesSize(FileStatus[] fileStatuses) {
        long allSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            allSize += fileStatus.getLen();
        }

        return allSize;
    }

    /**
     * 获取合并后的文件名
     * 
     * @param fileStatuses
     *            文件列表信息
     * @return 合并文件名
     */
    private String getCompactFileName(FileStatus[] fileStatuses) {
        String fileName = null;
        for (FileStatus fileStatus : fileStatuses) {
            String name = fileStatus.getPath().getName();

            if (fileName == null || fileName.compareTo(name) < 1) {
                fileName = name;
            }
        }

        return fileName;
    }
}
