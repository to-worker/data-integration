package com.zqykj.tldw.common;

import com.zqykj.hyjj.entity.elp.ElpModelDBMapping;
import com.zqykj.hyjj.entity.elp.Entity;
import com.zqykj.hyjj.entity.elp.Link;

import java.util.List;
import java.util.Map;

/**
 * @author feng.wei
 * @date 2018/5/10
 */
public class ElpDBMappingCache {

    /**
     * 卡口通行记录映射的列名和elp属性的对应关系
     */
    public static Map<String, String> BAYONET_ELPTYPE_COLUMN_MAP = null;

    /**
     * 车辆映射的列名和elp属性的对应关系
     */
    public static Map<String, String> VEHICLE_ELPTYPE_COLUMN_MAP = null;

    /**
     * 卡口通行记录接入列名
     */
    public static List<String> BAYONET_COLUMNS = null;

    /**
     * 车辆接入列名
     */
    public static List<String> VEHICLE_COLUMNS = null;

    /**
     * 数据源与模型映射关系
     */
    public static Map<String, ElpModelDBMapping> ELPMODEL_DBMAPPINGS = null;

    /**
     * 模型实体
     */
    public static Map<String, Entity> ELP_MODEL_ENTITY_PROPERTY = null;

    /**
     * 模型连接
     */
    public static Map<String, Link> ELP_MODEL_LINK_PROPERTY = null;

}
