package com.impetus.client.couchdb;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.RequestLine;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;

public class CouchDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBClientFactory.class);

    private HttpClient httpClient;

    private HttpHost httpHost;
    
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if (logger.isInfoEnabled())
        {
            logger.info("SchemaManager is not implemented for CouchDB");
        }
        return null;
    }

    @Override
    public void destroy()
    {
        httpClient.getConnectionManager().shutdown();
    }

    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new CouchDBEntityReader();
        initializePropertyReader();
        setExternalProperties(externalProperty);
    }

    @Override
    protected Object createPoolOrConnection()
    {
        // DefaultHttpClient httpclient = null;
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
            httpClient = new DefaultHttpClient(ccm);
            httpHost = new HttpHost(/* props.getHost() */"localhost", /*
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
            httpClient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");
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
            ((DefaultHttpClient) httpClient).addRequestInterceptor(new HttpRequestInterceptor()
            {
                public void process(final HttpRequest request, final HttpContext context) throws IOException
                {
                    if (logger.isInfoEnabled())
                    {
                        RequestLine requestLine = request.getRequestLine();
                        logger.info(">> " + requestLine.getMethod() + " " + URI.create(requestLine.getUri()).getPath());
                    }
                }
            });
            // response interceptor
            ((DefaultHttpClient) httpClient).addResponseInterceptor(new HttpResponseInterceptor()
            {
                public void process(final HttpResponse response, final HttpContext context) throws IOException
                {
                    if (logger.isInfoEnabled())
                        logger.info("<< Status: " + response.getStatusLine().getStatusCode());
                }
            });
        }
        catch (Exception e)
        {
            logger.error("Error Creating HTTP client. " + e.getMessage());
            throw new IllegalStateException(e);
        }
        return httpClient;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new CouchDBClient(httpClient, httpHost, reader, persistenceUnit, externalProperties, clientMetadata);
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new CouchDBPropertyReader(externalProperties);
            propertyReader.read(getPersistenceUnit());
        }
    }
}