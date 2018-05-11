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
    public void testGetUTCTime(){
        System.out.println(DateUtils.getUTCTime(-7));
    }

}
