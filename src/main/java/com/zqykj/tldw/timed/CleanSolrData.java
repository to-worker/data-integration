package com.zqykj.tldw.timed;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.solr.SolrClient;
import com.zqykj.tldw.util.DateUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Created by weifeng on 2018/5/11.
 */
@Service
public class CleanSolrData {

    @Value("")
    String zkHost;

    @Value("")
    String collection;

    @Value("")
    String relatoinType;

    @Value("")
    Integer days;

    @Scheduled(cron = "0 0 23 * * ?")
    public void clearDataByQuery() {
        SolrClient solrClient = new SolrClient(zkHost, collection);
        // "_indexed_at_tdt: [* TO \"2018-05-11T09:08:00.089Z\"]  AND relation_type:bayonet_pass_record"
        String utcTime = DateUtils.getUTCTime(days);
        String timeSlot = "[* TO \"" + utcTime + "\"]";
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("_indexed_at_tdt").append(Constants.QUERY_CONDITION_OPERATOR).append(timeSlot)
                .append(" AND ")
                .append("relation_type").append(Constants.QUERY_CONDITION_OPERATOR).append(relatoinType);

        solrClient.deleteByQuery(stringBuffer.toString());
    }

}
