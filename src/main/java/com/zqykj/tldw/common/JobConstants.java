package com.zqykj.tldw.common;

import com.zqykj.tldw.graph.core.query.db.GraphConstants;

public class JobConstants {

    public static final String FORMATTER_DATE = "yyyy-MM-dd";
    public static final String FORMATTER_DATETIME = "yyyy-MM-dd HH:mm:ss";
    public static final String FORMATTER_TIME = "HH:mm:ss";

    public static final String HBASE_TABLE_ROWKEY = GraphConstants.HBASE_TABLE_ROWKEY;
    public static final String VERTEX_LABEL = GraphConstants.VERTEX_LABEL_FILED;
    public static final String VERTEX_TYPE = GraphConstants.VERTEX_TYPE_FILED;
    public static final String VERTEX_ID = GraphConstants.VERTEX_ID_FILED;

    public static final String EDGE_TYPE = GraphConstants.EDGE_TYPE_FIELD;
    public static final String EDGE_ID = GraphConstants.EDGE_ID_FIELD;
    public static final String EDGE_LABEL = GraphConstants.EDGE_LABEL_FIELD;
    public static final String EDGE_FROM_VERTEX_TYPE = GraphConstants.EDGE_FROM_VERTEX_TYPE_FIELD;
    public static final String EDGE_FROM_VERTEX_ID = GraphConstants.EDGE_FROM_VERTEX_ID_FIELD;
    public static final String EDGE_TO_VERTEX_TYPE = GraphConstants.EDGE_TO_VERTEX_TYPE_FIELD;
    public static final String EDGE_TO_VERTEX_ID = GraphConstants.EDGE_TO_VERTEX_ID_FIELD;
    public static final String EDGE_DIRECTION_TYPE = GraphConstants.EDGE_DIRECTION_TYPE_FIELD;

    public final static String ID_SPACE_MARK = "~`#";

    public static final String ELP_DS_MAPPING_KEY = "dsMapping";

    /**
     * 无向
     */
    public static final String DIRECTION_UNDIRECTED = "0";

    /**
     * 单向
     */
    public static final String DIRECTION_UNIDIRECTIONAL = "1";

    /**
     * 双向
     */
    public static final String DIRECTION_BIDIRECTIONAL = "2";
}
