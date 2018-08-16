package com.zqykj.tldw.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.common.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * operate hdfs file or directory util class
 * 
 * @author zhangsz
 * @since 2017-03-11
 * @version v0.9
 */
public class HdfsUtils {

    public static final String HADOOP_CORE_SITE_FILE = "./config/cluster/hadoop/core-site.xml";
    public static final String HADOOP_HDFS_SITE_FILE = "./config/cluster/hadoop/hdfs-site.xml";
    public static final String KERBEROS_KRB5_CONF_FILE = "./config/kerberos/krb5.conf";
    public static final String KERBEROS_KEYTAB_FILE = "./config/kerberos/hdfs.keytab";

    /**
     * make a new dir in the hdfs
     * 
     * @param dir
     *            the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException
     *                something wrong happends when operating files
     */
    public static boolean mkdir(String hdfsUri, String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = hdfsUri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(dir), conf);
            if (!fs.exists(new Path(dir))) {
                fs.mkdirs(new Path(dir));
            }

        } finally {
            if (fs != null) {
                fs.close();
            }
        }

        return true;
    }

    /**
     * delete a dir in the hdfs. if dir not exists, it will throw
     * FileNotFoundException
     * 
     * @param dir
     *            the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException
     *                something wrong happends when operating files
     * 
     */
    public static boolean deleteDir(String hdfsUri, String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = hdfsUri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(dir), conf);
            Path path = new Path(dir);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }

        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return true;
    }

    /**
     * list files/directories/links names under a directory, not include embed
     * objects
     * 
     * @param dir
     *            a folder path may like '/tmp/testdir'
     * @return List<String> list of file names
     * @throws IOException
     *             file io exception
     */
    public static List<String> listAll(String hdfsUri, String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = hdfsUri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        List<String> names = new ArrayList<String>();
        try {
            fs = FileSystem.get(URI.create(dir), conf);
            FileStatus[] stats = fs.listStatus(new Path(dir));
            for (int i = 0; i < stats.length; ++i) {
                if (stats[i].isFile()) {
                    // regular file
                    names.add(stats[i].getPath().toString());
                } else if (stats[i].isDirectory()) {
                    // dir
                    names.add(stats[i].getPath().toString());
                } else if (stats[i].isSymlink()) {
                    // is s symlink in linux
                    names.add(stats[i].getPath().toString());
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }

        return names;
    }

    public static List<String> listAllTimeDesc(String hdfsUri, String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = hdfsUri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        List<String> names = new ArrayList<String>();
        try {
            fs = FileSystem.get(URI.create(dir), conf);
            FileStatus[] stats = fs.listStatus(new Path(dir));
            Collections.sort(Arrays.asList(stats), new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus o1, FileStatus o2) {
                    int time = Long.valueOf(o2.getModificationTime()).compareTo(Long.valueOf(o1.getModificationTime()));
                    if (0 == time) {
                        return o2.getPath().getName().compareTo(o1.getPath().getName());
                    }
                    return time;
                }
            });
            for (int i = 0; i < stats.length; ++i) {
                if (stats[i].isFile()) {
                    // regular file
                    names.add(stats[i].getPath().toString());
                } else if (stats[i].isDirectory()) {
                    // dir
                    names.add(stats[i].getPath().toString());
                } else if (stats[i].isSymlink()) {
                    // is s symlink in linux
                    names.add(stats[i].getPath().toString());
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }

        return names;
    }

    /*
     * upload the local file to the hds, notice that the path is full like
     * /tmp/test.txt if local file not exists, it will throw a
     * FileNotFoundException
     * 
     * @param localFile local file path, may like F:/test.txt or
     * /usr/local/test.txt
     * 
     * @param hdfsFile hdfs file path, may like /tmp/dir
     * 
     * @return boolean true-success, false-failed
     * 
     * @throws IOException file io exception
     */
    public static boolean uploadLocalFile2HDFS(String hdfsUri, String localFile, String hdfsFile) throws IOException {
        if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = hdfsUri + hdfsFile;
        Configuration config = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(URI.create(hdfsUri), config);
            Path src = new Path(localFile);
            Path dst = new Path(hdfsFile);
            hdfs.copyFromLocalFile(src, dst);
        } finally {
            if (hdfs != null) {
                hdfs.close();
            }
        }
        return true;
    }

    public static boolean uploadLocalFile2HDFS(boolean isKerberosEnable, String kerberosLoginUser,
            String hdfsUri, String hdfsFile, InputStream inputStream)
            throws IOException {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        Configuration config = new Configuration();
        FileSystem hdfs = null;
        try {
            if (isKerberosEnable){
                System.setProperty("java.security.krb5.conf", HdfsUtils.KERBEROS_KRB5_CONF_FILE);
                config.set("hadoop.security.authentication","kerberos");
                config.addResource(HdfsUtils.HADOOP_CORE_SITE_FILE);
                config.addResource(HdfsUtils.HADOOP_HDFS_SITE_FILE);
                UserGroupInformation.setConfiguration(config);
                UserGroupInformation.loginUserFromKeytab(kerberosLoginUser, HdfsUtils.KERBEROS_KEYTAB_FILE);
                UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
            }
            hdfs = FileSystem.get(URI.create(hdfsUri), config);
            FSDataOutputStream outputStream = hdfs.create(new Path(hdfsFile), true);
            IOUtils.copyBytes(inputStream, outputStream, 4096, true);
        } finally {
            if (hdfs != null) {
                hdfs.close();
            }
        }
        return true;
    }

    /*
     * create a new file in the hdfs.
     * 
     * notice that the toCreateFilePath is the full path
     * 
     * and write the content to the hdfs file.
     */
    /**
     * create a new file in the hdfs. if dir not exists, it will create one
     * 
     * @param newFile
     *            new file path, a full path name, may like '/tmp/test.txt'
     * @param content
     *            file content
     * @return boolean true-success, false-failed
     * @throws IOException
     *             file io exception
     */
    public static boolean createNewHDFSFile(String hdfsUri, String newFile, String content) throws IOException {
        if (StringUtils.isBlank(newFile) || null == content) {
            return false;
        }
        newFile = hdfsUri + newFile;
        Configuration config = new Configuration();
        FileSystem hdfs = null;
        FSDataOutputStream os = null;
        try {
            hdfs = FileSystem.get(URI.create(newFile), config);
            os.write(content.getBytes("UTF-8"));
        } finally {
            if (hdfs != null) {
                hdfs.close();
            }
            if (os != null) {
                os.close();
            }
        }
        return true;
    }

    /**
     * delete the hdfs file
     * 
     * @param hdfsFile
     *            a full path name, may like '/tmp/test.txt'
     * @return boolean true-success, false-failed
     * @throws IOException
     *             file io exception
     */
    public static boolean deleteHDFSFile(String hdfsUri, String hdfsFile) throws IOException {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = hdfsUri + hdfsFile;
        Configuration config = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(URI.create(hdfsFile), config);
            Path path = new Path(hdfsFile);
            return hdfs.delete(path, true);
        } finally {
            if (hdfs != null) {
                hdfs.close();

            }
        }
    }

    /**
     * If the name of hdfs's jar file is different from the new jar file ,
     * delete the first
     * 
     * @param hdfsUri
     * @param dir
     * @param jarFileName
     * @throws IOException
     */
    public static void deleteJarFile(boolean isKerberosEnable, String kerberosLoginUser,
            String hdfsUri, String dir, String jarFileName) throws IOException {
        if (StringUtils.isBlank(dir) || jarFileName == null) {
            return;
        }
        dir = hdfsUri + dir;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(dir), conf);
            FileStatus[] stats = fs.listStatus(new Path(dir));
            for (int i = 0; i < stats.length; ++i) {
                if (stats[i].isFile()) {
                    String fileName = stats[i].getPath().getName().toString();
                    if (fileName.contains("jar") && (!fileName.equals(jarFileName))) {
                        Path path = new Path(dir + File.separator + fileName);
                        if (fs.exists(path)) {
                            fs.delete(path, true);
                        }
                    }
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }

    }

    /**
     * read the hdfs file content
     * 
     * @param hdfsFile
     *            a full path name, may like '/tmp/test.txt'
     * @return byte[] file content
     * @throws IOException
     *             file io exception
     */
    public static byte[] readHDFSFile(String hdfsUri, String hdfsFile) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }
        hdfsFile = hdfsUri + hdfsFile;
        Configuration conf = new Configuration();
        FileSystem fs = null;
        FSDataInputStream is = null;
        try {
            fs = FileSystem.get(URI.create(hdfsFile), conf);
            // check if the file exists
            Path path = new Path(hdfsFile);
            if (fs.exists(path)) {
                is = fs.open(path);
                // get the file info to create the buffer
                FileStatus stat = fs.getFileStatus(path);
                // create the buffer
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                is.readFully(0, buffer);
                return buffer;
            } else {
                throw new Exception("the file is not found .");
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

    }

    /**
     * append something to file dst
     * 
     * @param hdfsFile
     *            a full path name, may like '/tmp/test.txt'
     * @param content
     *            string
     * @return boolean true-success, false-failed
     * @throws Exception
     *             something wrong
     */
    public static boolean append(String hdfsUri, String hdfsFile, String content) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        if (StringUtils.isEmpty(content)) {
            return true;
        }

        hdfsFile = hdfsUri + hdfsFile;
        Configuration conf = new Configuration();
        // solve the problem when appending at single datanode hadoop env
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        InputStream in = null;
        OutputStream out = null;
        try {
            // check if the file exists
            Path path = new Path(hdfsFile);
            if (fs.exists(path)) {
                try {
                    in = new ByteArrayInputStream(content.getBytes());
                    out = fs.append(new Path(hdfsFile));
                    IOUtils.copyBytes(in, out, 4096, true);

                } catch (Exception ex) {
                    fs.close();
                    throw ex;
                }
            } else {
                HdfsUtils.createNewHDFSFile(hdfsUri, hdfsFile, content);
            }
        } finally {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

        return true;
    }

}