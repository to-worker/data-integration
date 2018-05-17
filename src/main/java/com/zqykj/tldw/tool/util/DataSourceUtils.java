package com.zqykj.tldw.tool.util;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by fengwei on 17/5/8.
 */
public class DataSourceUtils {

    private static Logger logger = LoggerFactory.getLogger(DataSourceUtils.class);

    private static SqlSessionFactory sqlSessionFactory = null;
    private static final String mybatisConfig = "config/tool/mybatis-config.xml";

    private DataSourceUtils() {

    }

    static {
        if (sqlSessionFactory == null) {
            synchronized (DataSourceUtils.class) {
                if (sqlSessionFactory == null) {
                    InputStream inputStream = null;
                    try {
                        inputStream = new FileInputStream(new File(mybatisConfig));
                        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error("IOExceptoin: {}", e);
                    } finally {
                        try {
                            if (inputStream != null) {
                                inputStream.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            logger.error("IOExceptoin: {}", e);
                        }
                    }
                }
            }
        }
    }

    public static SqlSession getSession() {
        return sqlSessionFactory.openSession(true);
    }

    public static void close() {

    }
}
