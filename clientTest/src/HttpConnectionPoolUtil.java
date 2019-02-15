import com.google.gson.Gson;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.core.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpConnectionPoolUtil {

    private static Logger logger = LoggerFactory.getLogger(HttpConnectionPoolUtil.class);

    private static final int CONNECT_TIMEOUT = 10;// 设置连接建立的超时时间为10s
    private static final int SOCKET_TIMEOUT = 5;
    private static final int MAX_CONN = 100; // 最大连接数
    private static final int Max_PRE_ROUTE = 100;
    private static final int MAX_ROUTE = 100;
    private static CloseableHttpClient httpClient = null; // 发送请求的客户端单例
    private static PoolingHttpClientConnectionManager manager = null; //连接池管理类
    private static ScheduledExecutorService monitorExecutor = null;

    private final static Object syncLock = new Object(); // 相当于线程锁,用于线程安全

    /**
     * 对http请求进行基本设置
     * @param httpRequestBase http请求
     */
    private static void setRequestConfig(HttpRequestBase httpRequestBase){
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(CONNECT_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT).build();

        httpRequestBase.setConfig(requestConfig);
    }

    public static CloseableHttpClient getHttpClient(String url){
        String hostName = url.split("/")[2];
        //System.out.println(hostName);
        int port = 80;
        if (hostName.contains(":")){
            String[] args = hostName.split(":");
            hostName = args[0];
            port = Integer.parseInt(args[1]);
        }

        if (httpClient == null){
            //多线程下多个线程同时调用getHttpClient容易导致重复创建httpClient对象的问题,所以加上了同步锁
            synchronized (syncLock){
                if (httpClient == null){
                    httpClient = createHttpClient(hostName, port);
                    //开启监控线程,对异常和空闲线程进行关闭
                    monitorExecutor = Executors.newScheduledThreadPool(1);
                    monitorExecutor.scheduleAtFixedRate(new TimerTask() {
                        @Override
                        public void run() {
                            //关闭异常连接
                            manager.closeExpiredConnections();
                            //关闭5s空闲的连接
                            manager.closeIdleConnections(5000, TimeUnit.MILLISECONDS);
                            logger.info("close expired and idle for over 5s connection");
                        }
                    }, 5000, 5000, TimeUnit.MILLISECONDS);
                }
            }
        }
        return httpClient;
    }

    /**
     * 根据host和port构建httpclient实例
     * @param host 要访问的域名
     * @param port 要访问的端口
     * @return
     */
    public static CloseableHttpClient createHttpClient(String host, int port){
        ConnectionSocketFactory plainSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory> create().register("http", plainSocketFactory)
                .register("https", sslSocketFactory).build();

        manager = new PoolingHttpClientConnectionManager(registry);
        //设置连接参数
        manager.setMaxTotal(MAX_CONN); // 最大连接数
        manager.setDefaultMaxPerRoute(Max_PRE_ROUTE); // 路由最大连接数

        HttpHost httpHost = new HttpHost(host, port);
        manager.setMaxPerRoute(new HttpRoute(httpHost), MAX_ROUTE);

        //请求失败时,进行请求重试
        HttpRequestRetryHandler handler = new HttpRequestRetryHandler() {

            public boolean retryRequest(IOException e, int i, HttpContext httpContext) {
                if (i > 3){
                    //重试超过3次,放弃请求
                    logger.error("retry has more than 3 time, give up request");
                    return false;
                }
                if (e instanceof NoHttpResponseException){
                    //服务器没有响应,可能是服务器断开了连接,应该重试
                    logger.error("receive no response from server, retry");
                    return true;
                }
                if (e instanceof SSLHandshakeException){
                    // SSL握手异常
                    logger.error("SSL hand shake exception");
                    return false;
                }
                if (e instanceof InterruptedIOException){
                    //超时
                    logger.error("InterruptedIOException");
                    return false;
                }
                if (e instanceof UnknownHostException){
                    // 服务器不可达
                    logger.error("server host unknown");
                    return false;
                }
                if (e instanceof ConnectTimeoutException){
                    // 连接超时
                    logger.error("Connection Time out");
                    return false;
                }
                if (e instanceof SSLException){
                    logger.error("SSLException");
                    return false;
                }

                HttpClientContext context = HttpClientContext.adapt(httpContext);
                HttpRequest request = context.getRequest();
                if (!(request instanceof HttpEntityEnclosingRequest)){
                    //如果请求不是关闭连接的请求
                    return true;
                }
                return false;
            }
        };

        CloseableHttpClient client = HttpClients.custom().setConnectionManager(manager).setRetryHandler(handler).build();
        return client;
    }

    /**
     * 设置post请求的参数
     * @param httpPost
     * @param params
     */
    private static void setPostParams(HttpPost httpPost, Map<String, String> params){
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        Set<String> keys = params.keySet();
        for (String key: keys){
            nvps.add(new BasicNameValuePair(key, params.get(key)));
        }
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static JsonObject post(String url, Map<String, String> params){
        HttpPost httpPost = new HttpPost(url);
        setRequestConfig(httpPost);
        setPostParams(httpPost, params);
        CloseableHttpResponse response = null;
        InputStream in = null;
        JsonObject object = null;
        try {
            response = getHttpClient(url).execute(httpPost, HttpClientContext.create());
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                in = entity.getContent();
                String result = new String();
                while(0 < in.available()){
                    byte[] buf = new byte[1024];
                    in.read(buf);

                    result = result + buf;
                }
                Gson gson = new Gson();
                object = gson.fromJson(result, JsonObject.class);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try{
                if (in != null) in.close();
                if (response != null) response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return object;
    }

    public static boolean httpPostWithJson(String body, String url, StringBuilder resultJson){
        boolean isSuccess = false;

        HttpPost post = null;
        HttpClient httpClient = null;
        HttpResponse response = null;
        try {
            httpClient =  getHttpClient(url);

            post = new HttpPost(url);
            post.setHeader("Content-type", "application/json");

            StringEntity entity = new StringEntity(body, Charset.forName("UTF-8"));
            post.setEntity(entity);

            response = httpClient.execute(post);

            // 检验返回码
            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode != HttpStatus.SC_OK){
                logger.info("请求出错: "+statusCode + " body:" + body + " url:" + url);
                isSuccess = false;

                logger.info(response.getStatusLine().getReasonPhrase());
            }else{
                isSuccess = true;

                InputStream in = response.getEntity().getContent();

                //LOG.info("test select here1 getContentLength:" + response.getEntity().getContentLength());
                while(0 < in.available()){
                    byte[] buf = new byte[1024];
                    in.read(buf);

                    resultJson.append(new String(buf));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            isSuccess = false;
        }finally{
        }
        return isSuccess;
    }

    public static boolean httpPut(String url, String fieldJson){
        HttpClient httpClient = getHttpClient(url);

        HttpPut put = new HttpPut(url);
        put.setHeader("Content-type", "application/json");

        StringEntity entity = new StringEntity(fieldJson, Charset.forName("UTF-8"));
        put.setEntity(entity);

        HttpResponse response = null;
        try {
            response = httpClient.execute(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 检验返回码
        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode != HttpStatus.SC_OK){
            logger.info("请求出错: "+statusCode + " body:" + fieldJson + " url:" + url);
            logger.info(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().getReasonPhrase());

            return false;
        }

        return true;
    }

    public static boolean httpGet(String url, StringBuilder resultJson){
        HttpClient httpClient = getHttpClient(url);

        HttpGet get = new HttpGet(url);
        get.setHeader("Content-type", "application/json");

        try {
            HttpResponse response = httpClient.execute(get);

            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode != HttpStatus.SC_OK){
                logger.info("not error! get doc not found: "+statusCode + " url: " + url);
                return false;
            }

            InputStream in = response.getEntity().getContent();
            while(0 < in.available()){
                byte[] buf = new byte[1024];
                in.read(buf);

                resultJson.append(new String(buf));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 关闭连接池
     */
    public static void closeConnectionPool(){
        try {
            httpClient.close();
            manager.close();
            monitorExecutor.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
