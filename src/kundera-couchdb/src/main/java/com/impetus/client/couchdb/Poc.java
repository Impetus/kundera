package com.impetus.client.couchdb;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.RequestLine;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Hello world!
 * 
 */
public class Poc
{
    private static final Log log = LogFactory.getLog(Poc.class);

    public static void main(String[] args) throws URISyntaxException, ClientProtocolException, IOException
    {
        // crudUsingEkTorp();

        Gson gson = new Gson();

        CouchUser user = new CouchUser();

        String id = UUID.randomUUID().toString();
        user.setId(id);
        user.setName("Kuldeep");
        user.setAge(24);

        System.out.println(id);
        JsonObject object = gson.toJsonTree(user).getAsJsonObject();

        System.out.println(object);

        object.addProperty("_id", "100");
        object.addProperty("rev", "1");

        System.out.println(object);

        CouchUser convertedUser = gson.fromJson(object, CouchUser.class);

        System.out.println(convertedUser.getName());

        // HttpClientConnection connection = new

        HttpClient client = createHttpClient();

        URI uri = new URI("http", null, "localhost", 5984, "/mydatabase/" + user.getId(), null, null);

        HttpPut put = new HttpPut(uri);

        StringEntity entity = new StringEntity(object.toString(), "UTF-8");
        entity.setContentType("application/json");
        put.setEntity(entity);

        HttpHost host = new HttpHost(/* props.getHost() */"localhost", /*
                                                                        * props.
                                                                        * getPort
                                                                        * ()
                                                                        */5984, /*
                                                                                 * props
                                                                                 * .
                                                                                 * getProtocol
                                                                                 * (
                                                                                 * )
                                                                                 */"http");
        HttpResponse response = client.execute(host, put, getContext());

        System.out.println(response);

        get(gson, client, uri);

        delete(gson, user, client);

    }

    private static void get(Gson gson, HttpClient client, URI uri) throws IOException, ClientProtocolException
    {
        HttpResponse response;
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        response = client.execute(get);

        Reader reader = new InputStreamReader(response.getEntity().getContent());
        System.out.println(response.getEntity().toString());
        CouchUser user2 = gson.fromJson(reader, CouchUser.class);

        System.out.println(user2.getName());
        System.out.println(user2.getAge());
    }

    private static void delete(Gson gson, CouchUser user, HttpClient client) throws URISyntaxException, IOException,
            ClientProtocolException
    {
        URI uri;
        HttpResponse response;
        HttpGet get;
        Reader reader;
        CouchUser user2;
        uri = new URI("http", null, "localhost", 5984, "/mydatabase/" + user.getId(), null, null);
        get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        response = client.execute(get);

        reader = new InputStreamReader(response.getEntity().getContent());
        System.out.println(response.getEntity().toString());
        JsonObject json = gson.fromJson(reader, JsonObject.class);

        JsonElement rev = json.get("_rev");
        System.out.println(rev);

        StringBuilder builder = new StringBuilder();
        builder.append("rev=");
        builder.append(rev.getAsString());
        String q = builder.toString();

        uri = new URI("http", null, "localhost", 5984, "/mydatabase/" + user.getId(), q, null);
        HttpDelete delete = new HttpDelete(uri);

        response = client.execute(delete);

        System.out.println(response);

        response.getEntity().getContent().close();

        q = "rev=1-f298936f8aee335303290978eb99e9dd";
        uri = new URI("http", null, "localhost", 5984, "/mydatabase/" + user.getId(), null, null);
        get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");

        System.out.println(uri);
        response = client.execute(get);

        reader = new InputStreamReader(response.getEntity().getContent());
        user2 = gson.fromJson(reader, CouchUser.class);
        System.out.println(user2.getName());
        System.out.println(user2.getAge());
    }

    private static void crudUsingEkTorp()
    {/*
        org.ektorp.http.HttpClient httpClient = null;
        try
        {
            httpClient = new StdHttpClient.Builder().url("http://localhost:5984").build();
        }
        catch (MalformedURLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        CouchDbConnector db = new StdCouchDbConnector("mydatabase", dbInstance);

        db.createDatabaseIfNotExists();

        CouchUser user = new CouchUser();

        String id = UUID.randomUUID().toString();
        user.setId(id);
        user.setName("KK");
        user.setRev("11");
        db.create(user);

        CouchUser foundUser = db.get(CouchUser.class, id);
        // System.out.println(foundUser.getName());
    */}

    private static HttpClient createHttpClient()
    {
        DefaultHttpClient httpclient = null;
        try
        {
            SchemeSocketFactory ssf = null;
            // if (props.getProtocol().equals("https"))
            // {
            // TrustManager trustManager = new X509TrustManager()
            // {
            // public void checkClientTrusted(X509Certificate[] chain, String
            // authType)
            // throws CertificateException
            // {
            // }
            //
            // public void checkServerTrusted(X509Certificate[] chain, String
            // authType)
            // throws CertificateException
            // {
            // }
            //
            // public X509Certificate[] getAcceptedIssuers()
            // {
            // return null;
            // }
            // };
            // SSLContext sslcontext = SSLContext.getInstance("TLS");
            // sslcontext.init(null, new TrustManager[] { trustManager }, null);
            // ssf = new SSLSocketFactory(sslcontext,
            // SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            // SSLSocket socket = (SSLSocket) ssf.createSocket(null);
            // socket.setEnabledCipherSuites(new String[] {
            // "SSL_RSA_WITH_RC4_128_MD5" });
            // }
            // else
            // {
            ssf = PlainSocketFactory.getSocketFactory();
            // }
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme(/* props.getProtocol() */"http", /*
                                                                                 * props
                                                                                 * .
                                                                                 * getPort
                                                                                 * (
                                                                                 * )
                                                                                 */5984, ssf));
            PoolingClientConnectionManager ccm = new PoolingClientConnectionManager(schemeRegistry);
            httpclient = new DefaultHttpClient(ccm);
            HttpHost host = new HttpHost(/* props.getHost() */"localhost", /*
                                                                            * props.
                                                                            * getPort
                                                                            * ()
                                                                            */5984, /*
                                                                                     * props
                                                                                     * .
                                                                                     * getProtocol
                                                                                     * (
                                                                                     * )
                                                                                     */"http");
            // Http params
            httpclient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");
            // httpclient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,
            // props.getSocketTimeout());

            // httpclient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
            // props.getConnectionTimeout());
            // int maxConnections = props.getMaxConnections();
            // if (maxConnections != 0)
            // {
            // ccm.setMaxTotal(maxConnections);
            // ccm.setDefaultMaxPerRoute(maxConnections);
            // }
            // if (props.getProxyHost() != null)
            // {
            // HttpHost proxy = new HttpHost(props.getProxyHost(),
            // props.getProxyPort());
            // httpclient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY,
            // proxy);
            // }
            // basic authentication
            // if (props.getUsername() != null && props.getPassword() != null)
            // {
            // httpclient.getCredentialsProvider().setCredentials(new
            // AuthScope(props.getHost(), props.getPort()),
            // new UsernamePasswordCredentials(props.getUsername(),
            // props.getPassword()));
            // props.clearPassword();
            // }
            // request interceptor
            httpclient.addRequestInterceptor(new HttpRequestInterceptor()
            {
                public void process(final HttpRequest request, final HttpContext context) throws IOException
                {
                    if (log.isInfoEnabled())
                    {
                        RequestLine requestLine = request.getRequestLine();
                        log.info(">> " + requestLine.getMethod() + " " + URI.create(requestLine.getUri()).getPath());
                    }
                }
            });
            // response interceptor
            httpclient.addResponseInterceptor(new HttpResponseInterceptor()
            {
                public void process(final HttpResponse response, final HttpContext context) throws IOException
                {
                    if (log.isInfoEnabled())
                        log.info("<< Status: " + response.getStatusLine().getStatusCode());
                }
            });
        }
        catch (Exception e)
        {
            log.error("Error Creating HTTP client. " + e.getMessage());
            throw new IllegalStateException(e);
        }
        return httpclient;
    }

    private static HttpContext getContext()
    {
        HttpHost host = new HttpHost(/* props.getHost() */"localhost", /*
                                                                        * props.
                                                                        * getPort
                                                                        * ()
                                                                        */5984, /*
                                                                                 * props
                                                                                 * .
                                                                                 * getProtocol
                                                                                 * (
                                                                                 * )
                                                                                 */"http");
        AuthCache authCache = new BasicAuthCache();
        authCache.put(host, new BasicScheme());

        HttpContext context = new BasicHttpContext();
        context.setAttribute(ClientContext.AUTH_CACHE, authCache);
        return context;
    }
}
