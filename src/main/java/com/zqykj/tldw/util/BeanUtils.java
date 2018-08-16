package com.zqykj.tldw.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * from netposa
 */
public class BeanUtils {

    private static Logger logger = LoggerFactory.getLogger(BeanUtils.class);

    public static byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            logger.error("converting object to byte array has error: {}", ex.getStackTrace());
        }
        return bytes;
    }

    public static Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException | ClassNotFoundException ex) {
            logger.error("byte array convert to object has error: {}", ex.getStackTrace());
        }
        return obj;
    }

    public static Map<String, String> getFields(Object object){
        Field[] fields = object.getClass().getDeclaredFields();
        Map<String, String> map = new HashMap<>();
        for (Field field: fields){
            String name = field.getName();
            String type = field.getGenericType().toString();
            logger.info("name:{}, type:{}", name, type);
            map.put(name, type.replace("class ", ""));
        }
        return map;
    }
}