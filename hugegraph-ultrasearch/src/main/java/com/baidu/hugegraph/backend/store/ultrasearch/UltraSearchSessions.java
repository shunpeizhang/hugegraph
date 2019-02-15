package com.baidu.hugegraph.backend.store.ultrasearch;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import com.ultracloud.ultrasearch.http.client.FeedClient;
import com.ultracloud.ultrasearch.http.client.FeedClientFactory;
import com.ultracloud.ultrasearch.http.client.SimpleLoggerResultCallback;
import com.ultracloud.ultrasearch.http.client.config.Cluster;
import com.ultracloud.ultrasearch.http.client.config.ConnectionParams;
import com.ultracloud.ultrasearch.http.client.config.Endpoint;
import com.ultracloud.ultrasearch.http.client.config.FeedParams;
import com.ultracloud.ultrasearch.http.client.config.SessionParams;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;

public class UltraSearchSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(UltraSearchSessions.class);

    private HugeConfig config;
    private String database;
    private boolean opened;

    public UltraSearchSessions(HugeConfig config, String database, String store) {
        super(database + "/" + store);
        //super(database);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    public HugeConfig config() {
        return this.config;
    }

    public String database() {
        return this.database;
    }

    /**
     * Try connect with specified database, will not reconnect if failed
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void open(HugeConfig config) throws Exception {
        super.getOrNewSession();
        this.opened = true;

    }

    @Override
    protected boolean opened() {
        return this.opened;
    }



    @Override
    protected synchronized Session newSession() {
        return new Session(config, database);
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    public synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    public void checkSessionConnected() {
        LOG.info("checkSessionConnected here1");

        Session session = this.session();
        E.checkState(session != null, "MySQL session has not been initialized");

        LOG.info("checkSessionConnected here2");

        E.checkState(!session.closed(), "MySQL session has been closed");

        LOG.info("checkSessionConnected here3");
    }

    public static class Operation {
        final public String documentId;
        final public CharSequence data;

        public Operation(String id, CharSequence data) {
            this.documentId = id;
            this.data = data;
        }
    }

    public class Session extends BackendSession {

        private BlockingQueue<Operation> operations = new ArrayBlockingQueue<Operation>(100);
        private FeedClient feedClient;
        private AtomicInteger pending = new AtomicInteger(0);
        private AtomicBoolean drain = new AtomicBoolean(false);
        private final CountDownLatch finishedDraining = new CountDownLatch(1);
        private boolean opened;
        private HugeConfig config;
        private String database;

        public Session(HugeConfig config, String database) {
            this.opened = false;
            this.config = config;
            this.database = database;

            this.open();
        }

        public String database() {
            return this.database;
        }

        public void open(){
            Endpoint endPoint = Endpoint.create(config.get(UltraSearchOptions.ULTRASEARCH_IP), config.get(UltraSearchOptions.ULTRASEARCH_PORT), false);
            SessionParams sessionParams = new SessionParams.Builder()
                    .addCluster(new Cluster.Builder().addEndpoint(endPoint).build())
                    .setConnectionParams(new ConnectionParams.Builder().setDryRun(false).build())
                    .setFeedParams(new FeedParams.Builder()
                            .setDataFormat(FeedParams.DataFormat.JSON_UTF8)
                            .build())
                    .build();

            this.feedClient = FeedClientFactory.create(sessionParams, new SimpleLoggerResultCallback(this.pending, 1));
            this.opened = true;
        }



        @Override
        public void close() {
            assert this.closeable();
            drain.set(true);
            try {
                finishedDraining.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public void clear() {
            synchronized (this){
                operations.clear();
            }
        }

        @Override
        public Integer commit() {
            throw new RuntimeException("Not supported by US");
        }

        @Override
        public boolean hasChanges() {
            return this.operations.size() > 0;
        }

        public JSONArray select(String sql){
            JSONObject sqlObj = new JSONObject();
            sqlObj.put("sql", sql + ";");
            sql = sqlObj.toString();

            LOG.info("select : " + sql);

            String url = new String("http://" +
                    config.get(UltraSearchOptions.ULTRASEARCH_IP) + ":" +
                    config.get(UltraSearchOptions.ULTRASEARCH_PORT) + "/search/");

            StringBuilder resultJson = new StringBuilder();
            boolean ret = HttpConnectionPoolUtil.httpPostWithJson(sql, url, resultJson);
            if(!ret || 0 == resultJson.toString().length()){
                LOG.error("select failed! resultJson: " + resultJson);
                return null;
            }

            LOG.info("resultJson : " + resultJson.toString());

            //解析出children
            JSONArray children = null;
            {
                JSONObject obj = JSONObject.fromObject(resultJson.toString());
                JSONObject root = obj.getJSONObject("root");
                if(null == root){
                    LOG.error("json error!");
                    return null;
                }

                //检测是否sql有错误
                JSONObject errors = root.getJSONObject("errors");
                if(!errors.isEmpty()){
                    LOG.error("execute failed!");
                    return null;
                }

                if(!root.has("children")){
                    LOG.info("not data");
                    return null;
                }

                //得到结果
                children = root.getJSONArray("children");


            }

            return children;
        }

        public JSONObject get(String tableName, String docID){
            LOG.info("get tableName:  " + tableName + "  docID: " + docID);

            String url = new String("http://" +
                    config.get(UltraSearchOptions.ULTRASEARCH_IP) + ":" +
                    config.get(UltraSearchOptions.ULTRASEARCH_PORT) + "/document/v1/" +
                    tableName + "/" + tableName + "/docid/" + docID);

            JSONObject fields = null;
            StringBuilder resultJson = new StringBuilder();
            if(!HttpConnectionPoolUtil.httpGet(url, resultJson)){
                LOG.error("HttpConnectionPoolUtil.httpGet failed! " + url);
                return null;
            }

            JSONObject obj = JSONObject.fromObject(resultJson.toString());
            fields = obj.getJSONObject("fields");
            if(null == fields){
                LOG.error("json error! " + resultJson.toString());
                return null;
            }

            return fields;
        }

        public void add(Operation op) {
            LOG.info("add documentId:  " + op.documentId + "  data: " + op.data);

            synchronized (this){
                //operations.add(op);
                pending.incrementAndGet();
                feedClient.stream(op.documentId, op.data);
            }
        }

        public void add(String docID, String opJson) {
            LOG.info("add1 documentId:  " + docID + "  data: " + opJson);

            Operation op = new Operation(docID, opJson);
            synchronized (this){
                //operations.add(op);
                pending.incrementAndGet();
                feedClient.stream(op.documentId, op.data);
            }
        }

        public void postDoc(String tableName, String id, String fieldJson){
            LOG.info("postDoc documentId:  " + id + "  data: " + fieldJson);

            String url = new String("http://" +
                    config.get(UltraSearchOptions.ULTRASEARCH_IP) + ":" +
                    config.get(UltraSearchOptions.ULTRASEARCH_PORT) + "/document/v1/" + tableName + "/" + tableName + "/docid/" + id);

            StringBuilder resultJson = new StringBuilder();
            boolean ret = HttpConnectionPoolUtil.httpPostWithJson(fieldJson, url, resultJson);
            if(!ret){
                LOG.error("postDoc failed! " + fieldJson);
                return;
            }
        }

        public boolean putDoc(String tableName, String id, String fieldJson){
            LOG.info("putDoc documentId:  " + id + "  data: " + fieldJson);

            String url = new String("http://" +
                    config.get(UltraSearchOptions.ULTRASEARCH_IP) + ":" +
                    config.get(UltraSearchOptions.ULTRASEARCH_PORT) + "/document/v1/" + tableName + "/" + tableName + "/docid/" + id);

            if(!HttpConnectionPoolUtil.httpPut(url, fieldJson)){
                LOG.error("HttpConnectionPoolUtil.httpPut failed! " + url + " " + fieldJson);
                return false;
            }

            return true;
        }

        public void delete(String docID) {
            JSONObject obj = new JSONObject();
            obj.put("remove", docID);

            Operation op = new Operation(docID, obj.toString());
            synchronized (this){
                operations.add(op);
            }

            LOG.info("add documentId:  " + op.documentId + "  data: " + op.data);
        }

        public void deleteWhere(String sql){
            JSONArray result = select(sql);
            if(null == result) return;

            for (int i = 0; i < result.size(); i++) {
                JSONObject item = result.getJSONObject(i);

                String docID = item.getString("id");
                delete(docID);
            }

            LOG.info("deleteWhere sql:  " + sql);
        }

        public String getDocID(String table, String id){
            return "id:" + table + ":" + table + "::" + id;
        }

        public boolean httpPostWithJson(String body, String url, StringBuilder resultJson){
            boolean isSuccess = false;

            HttpPost post = null;
            HttpClient httpClient = null;
            HttpResponse response = null;
            try {
                //HttpClient httpClient = new DefaultHttpClient();
                RequestConfig requestConfig =
                        RequestConfig.custom().setConnectTimeout(5000).setConnectionRequestTimeout(10000).build();
                httpClient =  HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

                // 设置超时时间
                //httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000);
                //httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 2000);
                //httpClient.getParams().setParameter(CoreConnectionPNames.SO_KEEPALIVE, true);

                post = new HttpPost(url);
                post.setHeader("Content-type", "application/json");

                StringEntity entity = new StringEntity(body, Charset.forName("UTF-8"));
                post.setEntity(entity);

                response = httpClient.execute(post);

                // 检验返回码
                int statusCode = response.getStatusLine().getStatusCode();
                if(statusCode != HttpStatus.SC_OK){
                    LOG.info("请求出错: "+statusCode + " body:" + body + " url:" + url);
                    isSuccess = false;

                    LOG.info(response.getStatusLine().getReasonPhrase());
                }else{
                    isSuccess = true;

                    InputStream in = response.getEntity().getContent();

                    //LOG.info("test select here1 getContentLength:" + response.getEntity().getContentLength());
                    while(0 < in.available()){
                        byte[] buf = new byte[1024];
                        in.read(buf);

                        resultJson.append(new String(buf));

                        //LOG.info("test select here2");
                    }

                    //LOG.info("test select here3");
                }
            } catch (Exception e) {
                e.printStackTrace();
                isSuccess = false;
            }finally{
                if(post != null){
                    post.releaseConnection();
                }

                if (httpClient != null) {
                    try {
                        ((CloseableHttpClient) httpClient).close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return isSuccess;
        }
    }
}
