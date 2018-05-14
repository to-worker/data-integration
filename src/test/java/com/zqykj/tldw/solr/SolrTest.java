package com.zqykj.tldw.solr;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.util.DateUtils;
import org.junit.Test;

/**
 * Created by weifeng on 2018/5/11.
 */
public class SolrTest {

    @Test
    public void testDeleteByQuery() {
        SolrClient solrClient = new SolrClient("172.30.6.36:2181/solr", Constants.SOLR_RELATION_COLLECTION);
        String query = "_indexed_at_tdt: [* TO \"2018-05-11T09:08:00.089Z\"]  AND relation_type:bayonet_pass_record";
        int statusCode = solrClient.deleteByQuery(query);
        System.out.println("statusCode: " + statusCode);
    }

    @Test
    public void testGetUTCTime() {
        System.out.println(DateUtils.getUTCTime(-1));
    }

    @Test
    public void testUTCTime() {
        //1、取得本地时间：
        final java.util.Calendar cal = java.util.Calendar.getInstance();
        System.out.println("取得本地时间:" + cal.getTime());
        //2、取得时间偏移量：
        final int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);
        System.out.println("取得时间偏移量:" + zoneOffset);
        //3、取得夏令时差：
        final int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);
        System.out.println("取得夏令时差:" + dstOffset);
        //4、从本地时间里扣除这些差量，即可以取得UTC时间：
        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        System.err.println("取得UTC时间:" + cal.getTime());
    }

}
