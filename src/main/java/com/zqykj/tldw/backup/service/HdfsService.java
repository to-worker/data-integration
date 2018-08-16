package com.zqykj.tldw.backup.service;

import com.zqykj.tldw.common.Constants;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

@Service
public class HdfsService {
    private static final Logger logger = LoggerFactory.getLogger(HdfsService.class);

    /**
     * backup dir in hdfs
     */
    @Value("${hdfs.backup.dir}")
    private String hdfsBackUpDir;

    @Value("${hdfs.fs.default}")
    private String hdfsFsDefault;

    /**
     * hdfs的文件系统
     */
    private FileSystem hdfsFileSystem;

    @PostConstruct
    public void init() throws IOException, URISyntaxException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsFsDefault);
        //config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        config.set("fs.hdfs.impl.disable.cache","true");
//        URI uri = new URI(hdfsFsDefault);
        hdfsFileSystem = FileSystem.get(config);
    }

    /**
     * 目录创建
     * 
     * @param relativePath
     *            相对地址
     * @throws Exception
     */
    public void mkdir(String relativePath) throws Exception {
        Path path = getRealPath(relativePath);
        // 目录存在,则删除,清除目录下的内容
        if (hdfsFileSystem.exists(path)) {
            hdfsFileSystem.delete(path, true);
        }

        boolean isCreate = hdfsFileSystem.mkdirs(path);
        if (!isCreate) {
            throw new Exception("hdfs目录创建失败:" + relativePath);
        }
    }

    /**
     * 创建文件
     * 
     * @param relativePath
     *            相对地址
     * @param inputStream
     *            文件输入流
     * @throws Exception
     *             文件写入失败
     */
    public void createFile(String relativePath, InputStream inputStream) throws Exception {
        FSDataOutputStream dataOutputStream = hdfsFileSystem.create(getRealPath(relativePath), true);
        // 只关闭输出流,输入流,由外部关闭
        IOUtils.copyBytes(inputStream, dataOutputStream, 4096, false);
        dataOutputStream.close();
    }
    
    /**
     * 创建文件
     * @param path 路径
     * @return 文件流
     * @throws IOException 异常
     */
    public FSDataOutputStream createFile(Path path) throws IOException {
        return hdfsFileSystem.create(path, true);
    }

    /**
     * 拼接hdfs全路径
     * 
     * @param relativePath
     *            相对路径
     * @return 全路径
     */
    public Path getRealPath(String relativePath) {
        String realPath = hdfsBackUpDir + File.separator + relativePath;
        return new Path(realPath);
    }

    /**
     * 上传本地文件到hdfs上
     * 
     * @param hdfsRelativePath
     *            hdfs相对路径
     * @param localFile
     *            本地文件
     * @return 上传成功与否
     */
    public boolean uploadLocalFile(String hdfsRelativePath, String localFile) {
        if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsRelativePath)) {
            return false;
        }

        Path dstTemp = null;
        try {
            Path src = new Path(localFile);
            Path dst = getRealPath(hdfsRelativePath);
            dstTemp = getRealPath(hdfsRelativePath + Constants.FILE_NAME_TEMP);

            hdfsFileSystem.copyFromLocalFile(src, dstTemp);
            hdfsFileSystem.rename(dstTemp, dst);

            logger.info("上传文件:{} 到hdfs:{} 成功", localFile, hdfsRelativePath);
        } catch (Exception e) {
            logger.error("上传文件:{} 到hdfs:{} 失败:{}", localFile, hdfsRelativePath, e);

            return false;
        } finally {
            
            try {
                hdfsFileSystem.delete(dstTemp, false);
            } catch (IOException e) {
                logger.error("删除临时上传文件失败:{}:{}", dstTemp, e);
            }
        }
        return true;
    }

    /**
     * 列出文件列表
     * 
     * @param hdfsRelativePath
     *            hdfs相对目录
     * @return 文件列表信息
     * @throws Exception
     *             异常
     */
    public FileStatus[] list(String hdfsRelativePath) throws Exception {
        Path path = getRealPath(hdfsRelativePath);
        return list(path);
    }

    /**
     * 列出文件列表
     * 
     * @param path
     *            hdfs路径
     * @return 文件列表信息
     * @throws Exception
     *             异常
     */
    public FileStatus[] list(Path path) throws Exception {
        if (!hdfsFileSystem.exists(path)) {
            return null;
        }

        return hdfsFileSystem.listStatus(path);
    }

    /**
     * 列出文件列表
     * 
     * @param path
     *            hdfs路径
     * @return 文件列表信息
     * @throws Exception
     *             异常
     */
    public FileStatus[] list(Path path, final String endWith) throws Exception {
        if (!hdfsFileSystem.exists(path)) {
            return null;
        }

        return hdfsFileSystem.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(endWith);
            }
        });
    }

    /**
     * 合并文件
     * 
     * @param srcDirPath
     *            原目录
     * @param dstFile
     *            目标文件
     * @throws IOException
     *             异常
     */
    public void mergeFiles(Path srcDirPath, String dstFile, FileStatus[] srcFiles) throws Exception {
        Path dstFilePath = getRealPath(dstFile);
        Path dstFilePathTemp = getRealPath(dstFile + Constants.FILE_NAME_TEMP);
        
        DataFileWriter<GenericRecord> writer = null;
        try {
            writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
            
            Schema schema = null;
            String inputCodec = null;
            for (FileStatus srcFile : srcFiles) {
                DataFileStream<GenericRecord> reader = null;
                try {
                    reader = new DataFileStream<GenericRecord>(hdfsFileSystem.open(srcFile.getPath()), new GenericDatumReader<GenericRecord>());

                    if (schema == null) {
                        schema = reader.getSchema();
                        for (String key : reader.getMetaKeys()) {
                            if (!DataFileWriter.isReservedMeta(key)) {
                                byte[] metadatum = reader.getMeta(key);
                                writer.setMeta(key, metadatum);
                            }
                        }
                        inputCodec = reader.getMetaString(Constants.AVRO_CODEC);
                        if (inputCodec == null) {
                            inputCodec = "null";
                        }
                        writer.setCodec(CodecFactory.fromString(inputCodec));
                        writer.create(schema, createFile(dstFilePathTemp));
                    }

                    // 写入数据
                    writer.appendAllFrom(reader, false);
                    // 删除原文件
                    hdfsFileSystem.delete(srcFile.getPath(), false);
                } catch (Exception e) {
                    logger.error("合并文件:{} 到:{}, 失败:{}", srcFile.getPath(), dstFilePathTemp.getName(), e);
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            }
            
            // 文件重命名
            hdfsFileSystem.rename(dstFilePathTemp, dstFilePath);
        } catch (Exception e) {
            logger.error("合并文件失败:{}", e);
            
            // 删除临时文件
            hdfsFileSystem.delete(dstFilePathTemp, false);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 删除hdfs文件
     * @param realPath 全路径
     * @throws IOException 异常
     */
	public void deleteFile(String realPath) {
		Path path = new Path(realPath);	
        try {
			if (hdfsFileSystem.exists(path)) {
			    hdfsFileSystem.delete(path, true);
			}
		} catch (IOException e) {
			logger.error("删除目录失败", e);
		}
	}
	
	/**
     * 删除hdfs文件
     * @param relativePath
     * @throws IOException 
     */
    public void deleteDir(String relativePath) {
        Path path = getRealPath(relativePath);  
        try {
            if (hdfsFileSystem.exists(path)) {
                hdfsFileSystem.delete(path, true);
            }
        } catch (IOException e) {
            logger.error("删除目录失败", e);
        }
    }

	/**
     * 获取DataFileStream<GenericRecord> reader
     * @param sampleFile
     * @throws IOException 
     */
	public DataFileStream<GenericRecord> getDataFileStream(String sampleFile) throws IOException {
		Path path = getRealPath(sampleFile);
        DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(hdfsFileSystem.open(path), new GenericDatumReader<GenericRecord>());
		return reader;
	}
}
