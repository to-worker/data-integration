package com.zqykj.tldw.solr;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.Collection;

/**
 * @author feng.wei
 * @date 2018/5/8
 */
public class SolrClient {

    private static Logger log = LoggerFactory.getLogger(SolrClient.class);

    private CloudSolrClient cloudSolrClient;
    private String collection;

    public SolrClient(String zkHost, String collectin) {
        this.collection = collectin;
        this.cloudSolrClient = new CloudSolrClient(zkHost);
        this.cloudSolrClient.setDefaultCollection(collectin);
    }

    public void sendBatchToSolr(Collection<SolrInputDocument> batch) {
        UpdateRequest req = new UpdateRequest();
        req.setParam("collection", collection);
        req.add(batch);
        try {
            cloudSolrClient.request(req);
        } catch (Exception e) {
            if (shouldRetry(e)) {
                log.error("Send batch to collection " + collection + " failed due to " + e + "; will retry ...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }

                try {
                    cloudSolrClient.request(req);
                } catch (Exception e1) {
                    log.error("Retry send batch to collection " + collection + " failed due to: " + e1, e1);
                    if (e1 instanceof RuntimeException) {
                        throw (RuntimeException) e1;
                    } else {
                        throw new RuntimeException(e1);
                    }
                }
            } else {
                log.error("Send batch to collection " + collection + " failed due to: " + e, e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            batch.clear();
        }

    }

    private static boolean shouldRetry(Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        return (rootCause instanceof ConnectException || rootCause instanceof SocketException);

    }

}
