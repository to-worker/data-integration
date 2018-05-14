package com.zqykj.tldw.timed;

import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.TldwConfig;
import com.zqykj.tldw.solr.SolrClient;
import com.zqykj.tldw.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * Created by weifeng on 2018/5/11.
 */
@Service
public class CleanSolrData {

    private static Logger logger = LoggerFactory.getLogger(CleanSolrData.class);

    @Value("${solr.zk.host}")
    private String zkHost;

    @Value("${tldw.elp.type}")
    private String relatoinType;

    @Value("${solr.clean.interval}")
    private Integer days;

    // 23 clock at night: 0 0 23 * * ?
    // at interval 10 mins: 0 0/10 * * * ?
    @Scheduled(cron = "0 0 23 * * ?")
    public void clearDataByQuery() {
        logger.info("zkHost:{}, days: {}, relationType: {}", zkHost, days, relatoinType);
        SolrClient solrClient = null;
        try {
            solrClient = new SolrClient(zkHost, Constants.SOLR_RELATION_COLLECTION);
            // "_indexed_at_tdt: [* TO \"2018-05-11T09:08:00.089Z\"]  AND relation_type:bayonet_pass_record"
            String utcTime = DateUtils.getUTCTime(days);
            String timeSlot = "[* TO \"" + utcTime + "\"]";
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("_indexed_at_tdt").append(Constants.QUERY_CONDITION_OPERATOR).append(timeSlot).append(" AND ").append("relation_type").append(Constants.QUERY_CONDITION_OPERATOR)
                    .append(relatoinType);

            logger.info("clear data in solr: {}", stringBuffer.toString());
            int code = solrClient.deleteByQuery(stringBuffer.toString());
            logger.info("code of clear data task is {}", code);
        }catch(Exception e) {
            logger.error("clear data has exception");
        }finally {
            solrClient.close();
        }
    }

}
