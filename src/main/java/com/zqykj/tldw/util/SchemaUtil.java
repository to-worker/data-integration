package com.zqykj.tldw.util;

import org.apache.avro.Schema;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SchemaUtil {
    public static Schema getSchema(Map<String, String> columnMap, String schemaName) {
        StringBuffer schema = new StringBuffer();
        schema.append("{");

        schema.append("\"namespace\":\"com.zqykj\",").append("\"type\":\"record\",")
                .append("\"name\":\"" + schemaName + "\",").append("\"fields\":[");

        for (Map.Entry<String, String> column : columnMap.entrySet()) {
            schema.append("{\"name\":\"" + (String) column.getKey() + "\",\"type\":[\"" + "null\"" + ",\"" + (String) column.getValue()
                    + "\"]},");
        }

        schema.deleteCharAt(schema.length() - 1);

        schema.append("]").append("}");

        return new Schema.Parser().parse(schema.toString());
    }

    public static String tranceToAvroType(String dbColumnType) {
        if ("VARCHAR".equalsIgnoreCase(dbColumnType) || "VARCHAR2".equalsIgnoreCase(dbColumnType) || "CHAR"
                .equalsIgnoreCase(dbColumnType)) {
            return "string";
        }

        if ("TIME".equalsIgnoreCase(dbColumnType) || "DATETIME".equalsIgnoreCase(dbColumnType) || "TIMESTAMP"
                .equalsIgnoreCase(dbColumnType) || "DATE".equalsIgnoreCase(dbColumnType)) {
            return "long";
        }

        if (("INT".equalsIgnoreCase(dbColumnType)) || ("TINYINT UNSIGNED".equalsIgnoreCase(dbColumnType)) || ("TINYINT"
                .equalsIgnoreCase(dbColumnType))) {
            return "int";
        }

        // if ("DATE".equalsIgnoreCase(dbColumnType)) {
        // return "string";
        // }

        if ("BIGINT".equalsIgnoreCase(dbColumnType)) {
            return "long";
        }

        if ("NUMBER".equalsIgnoreCase(dbColumnType) || "DECIMAL".equalsIgnoreCase(dbColumnType) || "CLOB"
                .equalsIgnoreCase(dbColumnType)) {
            return "string";
        }

        return dbColumnType;
    }

    public static String tranceToAvroType(ResultSetMetaData metaData, int index) throws Exception {
        switch (metaData.getColumnType(index)) {
        case Types.CHAR:
        case Types.NCHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return "string";

        case Types.CLOB:
        case Types.NCLOB:
            return "string";

        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return "long";

        case Types.NUMERIC:
        case Types.DECIMAL:
            return "string";

        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
            return "string";

        case Types.TIME:
            return "long";

        case Types.DATE:
            return "long";

        case Types.TIMESTAMP:
            return "long";

        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
        case Types.LONGVARBINARY:
            return "bytes";

        case Types.BOOLEAN:
        case Types.BIT:
            return "boolean";

        case Types.NULL:
        default:
            throw new Exception("不支持转换成avro类型");
        }
    }

    public static String transToAvroType(String type) throws Exception {
        // null, boolean, int, long, float, double, bytes, and string
        switch (type) {
        case "long":
            return "long";
        case "java.lang.String":
            return "string";
        case "[B":
            return "string";
        case "double":
            return "double";
        case "int":
            return "int";
        case "java.lang.Integer":
            return "int";
        case "java.lang.Boolean":
            return "boolean";
        default:
            throw new Exception(type + ": 不支持转换成avro类型");
        }
    }

    public static Map<String, String> getAvroNameTypeMap(Map<String, String> map) throws Exception {
        Map<String, String> avroNameTypeMap = new HashMap<>();
        Iterator<String> iterator = map.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            String type = map.get(key);
            String avroType = transToAvroType(type);
            avroNameTypeMap.put(key, avroType);
        }
        return avroNameTypeMap;
    }

    public static String formatKey(String columnName) {
        if (columnName.contains("/")) {
            columnName = columnName.replaceAll("/", "_");
        }

        if (columnName.contains("(")) {
            columnName = columnName.replaceAll("\\(", "_");
        }

        if (columnName.contains(")")) {
            columnName = columnName.replaceAll("\\)", "_");
        }

        if (columnName.contains("{")) {
            columnName = columnName.replaceAll("\\{", "_");
        }

        if (columnName.contains("}")) {
            columnName = columnName.replaceAll("\\}", "_");
        }

        if (columnName.contains("[")) {
            columnName = columnName.replaceAll("\\[", "_");
        }

        if (columnName.contains("]")) {
            columnName = columnName.replaceAll("\\]", "_");
        }

        if (columnName.contains("-")) {
            columnName = columnName.replaceAll("\\-", "_");
        }

        columnName = firstCharNonum(columnName);

        return columnName;
    }

    private static String firstCharNonum(String columnName) {
        int index = -1;
        for (int i = 0; i < columnName.length(); i++) {
            String charString = columnName.substring(i, i + 1);
            if (charString.matches("[\\[\\]\\(\\)\\{\\}_\\d]")) {
                continue;
            }

            index = i;
            break;
        }

        if (index == 0) {
            return columnName;
        }

        return columnName.substring(index) + columnName.substring(0, index);
    }
}