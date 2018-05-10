package com.zqykj.tldw.bussiness;

import com.zqykj.hyjj.entity.elp.*;
import com.zqykj.hyjj.query.CompactLinkData;
import com.zqykj.hyjj.query.PropertyData;
import com.zqykj.tldw.common.JobConstants;
import com.zqykj.tldw.util.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by weifeng on 2018/5/10.
 */
public class ElpTransformer {

    private static Logger logger = LoggerFactory.getLogger(ElpTransformer.class);

    private static ElpPropertyTransformConfigService transCfgService = null;

    public static void init(String mongoIp, int mongoPort, String mongoDB){
        transCfgService = new ElpPropertyTransformConfigService(mongoIp, mongoPort, mongoDB);
    }

    /**
     * 解析连接成elp数据,并组装成Solr Document.
     *
     * @param v
     * @param elp
     * @param element
     * @return
     */
    public static SolrInputDocument parseLink(Map<String, Object> v, ElpModel elp, Link element, ElpModelDBMapping mapping) {
        String label = element.getLabelTemplate();
        Set<String> varNames = ELPUtils.parseVarNames(label);
        Map<String, String> varValues = new HashMap<>();

        CompactLinkData linkData = new CompactLinkData(element.getUuid());
        StringBuffer idStr = new StringBuffer(element.getUuid());
        for (Property prop : element.getProperties()) {
            String uuid = prop.getUuid();
            Object value = v.get(uuid);
            if (varNames.contains(prop.getName())) {
                varValues.put(prop.getName(), value == null ? "" : Utils.propToString(prop, value));
            }
            if (prop.isIdentifier()) {
                if (idStr.length() > 0) {
                    idStr.append(JobConstants.ID_SPACE_MARK);
                }
                idStr.append(Utils.propToString(prop, value));
            }
            linkData.addProperty(new PropertyData(prop.getName(), value));
        }
        // 判断并调整链接方向
        Directivity dataDirectivity = null;
        boolean needReverseDirection = false;
        if (Directivity.DirectionColumn.equals(mapping.getDirectivity()) && mapping.getDirectionColumn() != null) {
            dataDirectivity = mapping.getDirectionColumn()
                    .testLinkData(linkData, element.getProperty(mapping.getDirectionColumn().getColumnName()));
            if (null == dataDirectivity) {
                logger.warn("链接[{}]单一列[{}]值为空", idStr, mapping.getDirectionColumn().getColumnName());
                return getDummyDoc();
            }
            switch (dataDirectivity) {
            case TargetToSource:
                needReverseDirection = true;
                dataDirectivity = Directivity.SourceToTarget;
                break;
            default:
                break;
            }
        }
        if (mapping.getDirectivity() == Directivity.TargetToSource) {
            needReverseDirection = true;
        }
        if (dataDirectivity == null) {
            dataDirectivity = ((mapping.getDirectivity() == Directivity.TargetToSource) ?
                    Directivity.SourceToTarget :
                    mapping.getDirectivity());
        }

        SolrInputDocument doc = new SolrInputDocument();
        // docId:relation_id@relation_type
        doc.setField(JobConstants.HBASE_TABLE_ROWKEY, idStr.toString() + "@" + element.getUuid());
        if (!needReverseDirection) { // 不需要调整
            doc.setField(JobConstants.EDGE_FROM_VERTEX_TYPE, element.getSourceEntity());
            doc.setField(JobConstants.EDGE_FROM_VERTEX_ID,
                    parseEntityId(element, "source", elp.getEntityByUuid(element.getSourceEntity()), v));
            doc.setField(JobConstants.EDGE_TO_VERTEX_TYPE, element.getTargetEntity());
            doc.setField(JobConstants.EDGE_TO_VERTEX_ID,
                    parseEntityId(element, "target", elp.getEntityByUuid(element.getTargetEntity()), v));
        } else {
            doc.setField(JobConstants.EDGE_TO_VERTEX_TYPE, element.getSourceEntity());
            doc.setField(JobConstants.EDGE_TO_VERTEX_ID,
                    parseEntityId(element, "source", elp.getEntityByUuid(element.getSourceEntity()), v));
            doc.setField(JobConstants.EDGE_FROM_VERTEX_TYPE, element.getTargetEntity());
            doc.setField(JobConstants.EDGE_FROM_VERTEX_ID,
                    parseEntityId(element, "target", elp.getEntityByUuid(element.getTargetEntity()), v));
        }
        // 数据方向
        String directionType = JobConstants.DIRECTION_UNIDIRECTIONAL;
        switch (dataDirectivity) {
        case SourceToTarget:
        case TargetToSource:
            directionType = JobConstants.DIRECTION_UNIDIRECTIONAL;
            break;
        case NotDirected:
            directionType = JobConstants.DIRECTION_UNDIRECTED;
            break;
        case Bidirectional:
            directionType = JobConstants.DIRECTION_BIDIRECTIONAL;
            break;
        default:
            break;
        }
        doc.setField(JobConstants.EDGE_DIRECTION_TYPE, directionType);
        doc.setField(JobConstants.EDGE_TYPE, element.getUuid());
        doc.setField(JobConstants.EDGE_ID, idStr.toString());
        try {
            for (String varName : varNames) {
                label = label.replaceAll("\\$\\{" + varName + "\\}", varValues.get(varName));
            }
        } catch (Exception e) {
            logger.error("Error when parsing link label", e);
            logger.error("Template {0}, var names: {1}, var values {2}", label, varNames, varValues);
            label = "ERR:parsing";
        }
        doc.setField(JobConstants.EDGE_LABEL, label);
        for (Property p : element.getProperties()) {
            String key = p.getUuid();
            Object value = v.get(key);

            try {
                Object valueObj = getValue(key, value, element);
                if (null != valueObj) {
                    doc.setField(key, valueObj);
                }
            } catch (Throwable e) {
                logger.info("Transform error,key=" + key + "; value=" + value + "; entity=" + element + ".", e);
            }
        }
        return doc;

    }

    /**
     * 解析实体成elp数据,并组装成Solr Document.
     *
     * @param v
     * @param elp
     * @param element
     * @return
     */
    public static SolrInputDocument parseEntity(final Map<String, Object> v, final ElpModel elp, final Entity element) {
        String label = element.getLabelTemplate();
        Set<String> varNames = ELPUtils.parseVarNames(label);
        Map<String, String> varValues = new HashMap<>();

        StringBuffer idStr = new StringBuffer(element.getUuid());
        for (Property prop : element.getProperties()) {
            String uuid = prop.getUuid();
            Object value = v.get(uuid);
            if (varNames.contains(prop.getName())) {
                varValues.put(prop.getName(), value == null ? "" : Utils.propToString(prop, value));
            }
            if (prop.isIdentifier()) {
                if (idStr.length() > 0) {
                    idStr.append(JobConstants.ID_SPACE_MARK);
                }
                idStr.append(Utils.propToString(prop, value));
            }
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.setField(JobConstants.HBASE_TABLE_ROWKEY, idStr.toString() + "@" + element.getUuid());
        doc.setField(JobConstants.VERTEX_ID, idStr.toString());
        doc.setField(JobConstants.VERTEX_TYPE, element.getUuid());
        try {
            for (String varName : varNames) {
                label = label.replaceAll("\\$\\{" + varName + "\\}", varValues.get(varName));
            }
        } catch (Exception e) {
            logger.error("Error when parsing entity label", e);
            logger.error("Template {0}, var names: {1}, var values {2}", label, varNames, varValues);
            label = "ERR:parsing";
        }
        doc.setField(JobConstants.VERTEX_LABEL, label);
        for (Property p : element.getProperties()) {
            String key = p.getUuid();
            Object value = v.get(key);

            try {
                Object valueObj = getValue(key, value, element);
                if (null != valueObj) {
                    doc.setField(key, valueObj);
                }
            } catch (Throwable e) {
                logger.info("Transform error,key=" + key + "; value=" + value + "; entity=" + element + ".", e);
            }
        }
        return doc;
    }

    private static Object getValue(final String key, final Object value, final PropertyBag element) {
        if (null != value && value instanceof Long) {
            if (element.getPropertyByUUID(key).getType().equals(PropertyType.datetime)) {
                return new java.sql.Timestamp((Long) value);
            }
            if (element.getPropertyByUUID(key).getType().equals(PropertyType.date)) {
                return new Date((Long) value);
            }
            // 时间类型：12：30转换成字符串
            if (element.getPropertyByUUID(key).getType().equals(PropertyType.time)) {
                SimpleDateFormat sdf = new SimpleDateFormat(JobConstants.FORMATTER_TIME);
                return sdf.format(new Date((Long) value));
            }
        }
        if (null != value && value instanceof String) {
            if (element.getPropertyByUUID(key).getType().equals(PropertyType.number)) {
                return new Double((String) value);
            }
            if (element.getPropertyByUUID(key).getType().equals(PropertyType.integer)) {
                return new Integer((String) value);
            }

            if (element.getPropertyByUUID(key).getType().equals(PropertyType.date)) {
                    /* Add by liaoming 2017-8-27 foshan project */
                Date result = transCfgService
                        .parseDateString(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATE, (String) value);
                if (null == result) {
                        /* If all patterns mismatched,return null object. */
                    logger.info("Pattern mismatched,return null! Raw data=" + value + ", property type date.");
                }

                return result;
            }

            if (element.getPropertyByUUID(key).getType().equals(PropertyType.datetime)) {
                    /* Add by liaoming 2017-8-29 foshan project */
                Date result = transCfgService
                        .parseDateString(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATETIME, (String) value);
                if (null == result) {
                        /* If all patterns mismatched,return null object. */
                    logger.info("Pattern mismatched,return null! Raw data=" + value + ", property type datetime.");
                }

                return result;
            }
        }
        return value;
    }

    private static SolrInputDocument getDummyDoc() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField(JobConstants.HBASE_TABLE_ROWKEY, "-1");
        doc.setField(JobConstants.EDGE_FROM_VERTEX_TYPE, "-1");
        doc.setField(JobConstants.EDGE_FROM_VERTEX_ID, "-1");
        doc.setField(JobConstants.EDGE_TO_VERTEX_TYPE, "-1");
        doc.setField(JobConstants.EDGE_TO_VERTEX_ID, "-1");
        doc.setField(JobConstants.EDGE_DIRECTION_TYPE, "-1");
        doc.setField(JobConstants.EDGE_TYPE, "-1");
        doc.setField(JobConstants.EDGE_ID, "-1");
        doc.setField(JobConstants.EDGE_LABEL, "-1");
        return doc;
    }

    private static String parseEntityId(final Link l, final String type, final Entity foreign,
            final Map<String, Object> v) {
        /**
         * 根据链接的外键引用匹配出源（或目标）实体的主键。生成id
         */
        List<Link.PropertyForeignKey> propertyForeignKeys = l.getPropertyForeignKeys();
        if (CollectionUtils.isEmpty(propertyForeignKeys)) {
            return "";
        }
        TreeSet<Link.PropertyReference> prfs = null;
        for (Link.PropertyForeignKey pfk : propertyForeignKeys) {
            if (!foreign.getUuid().equals(pfk.getForeignEntity()) || !pfk.getName().equals(type)) {
                continue;
            }
            prfs = pfk.getReferences();
            break;
        }
        if (null == prfs || prfs.isEmpty()) {
            return "";
        }
        List<Property> properties = foreign.getProperties();
        StringBuffer idStr = new StringBuffer(foreign.getUuid());
        for (Property p : properties) {
            if (!p.isIdentifier()) {
                continue;
            }
            for (Iterator<Link.PropertyReference> it = prfs.iterator(); it.hasNext(); ) {
                Link.PropertyReference prf = it.next();
                if (prf.getForeignProperty().equals(p.getName())) {
                    if (idStr.length() > 0) {
                        idStr.append(Link.ID_SPACE_MARK);
                    }
                    Property lp = l.getProperty(prf.getLocalProperty());
                    Object value = v.get(lp.getUuid());
                    idStr.append(Utils.propToString(lp, value));
                    break;
                }
            }
        }
        return idStr.toString();
    }

}
