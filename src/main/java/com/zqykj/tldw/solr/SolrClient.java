package com.zqykj.tldw.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private String zkHost;

    public SolrClient(String zkHost, String collectin) {
        this.zkHost = zkHost;
        this.collection = collectin;
        initSolrCloudClient();
    }

    public void initSolrCloudClient() {
        this.cloudSolrClient = new CloudSolrClient(this.zkHost);
        this.cloudSolrClient.setDefaultCollection(this.collection);
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

    public int deleteByQuery(String query){
        int statusCode = -1;
        try {
            UpdateResponse response = cloudSolrClient.deleteByQuery(query);
            statusCode = response.getStatus();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return statusCode;
    }

    public void close() {
        if (null == cloudSolrClient) {
            try {
                cloudSolrClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
