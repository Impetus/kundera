package com.impetus.client.couchdb.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.HttpContext;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impetus.client.couchdb.CouchDBConstants;
import com.impetus.client.couchdb.CouchDBDesignDocument;
import com.impetus.client.couchdb.CouchDBDesignDocument.MapReduce;
import com.impetus.client.couchdb.CouchDBUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class CouchDBTestUtils
{
    public static HttpClient initiateHttpClient(String persistenceUnit)
    {
        PersistenceUnitMetadata pumMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);

        SchemeSocketFactory ssf = null;
        ssf = PlainSocketFactory.getSocketFactory();
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        int port = Integer.parseInt(pumMetadata.getProperty(PersistenceProperties.KUNDERA_PORT));
        String host = pumMetadata.getProperty(PersistenceProperties.KUNDERA_NODES);
        String userName = pumMetadata.getProperty(PersistenceProperties.KUNDERA_USERNAME);
        String password = pumMetadata.getProperty(PersistenceProperties.KUNDERA_PASSWORD);

        schemeRegistry.register(new Scheme("http", port, ssf));
        PoolingClientConnectionManager ccm = new PoolingClientConnectionManager(schemeRegistry);
        HttpClient httpClient = new DefaultHttpClient(ccm);

        try
        {
            // Http params
            httpClient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");

            // basic authentication
            if (userName != null && password != null)
            {
                ((AbstractHttpClient) httpClient).getCredentialsProvider().setCredentials(new AuthScope(host, port),
                        new UsernamePasswordCredentials(userName, password));
            }
            // request interceptor
            ((DefaultHttpClient) httpClient).addRequestInterceptor(new HttpRequestInterceptor()
            {
                public void process(final HttpRequest request, final HttpContext context) throws IOException
                {

                }
            });
            // response interceptor
            ((DefaultHttpClient) httpClient).addResponseInterceptor(new HttpResponseInterceptor()
            {
                public void process(final HttpResponse response, final HttpContext context) throws IOException
                {

                }
            });
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
        return httpClient;
    }

    public static void createDatabase(String databaseName, HttpClient client, HttpHost httpHost)
    {
        HttpResponse response = null;
        try
        {
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + databaseName, null, null);
            HttpPut put = new HttpPut(uri);
            response = client.execute(httpHost, put, CouchDBUtils.getContext(httpHost));
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
        catch (ClientProtocolException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }

    public static void dropDatabase(String databaseName, HttpClient client, HttpHost httpHost)
    {
        HttpResponse response = null;
        try
        {
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + databaseName, null, null);
            HttpDelete delete = new HttpDelete(uri);

            client.execute(httpHost, delete, CouchDBUtils.getContext(httpHost));
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
        catch (ClientProtocolException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }

    public static void createViews(String[] columns, String tableName, HttpHost httpHost, String databaseName,
            HttpClient httpClient) throws URISyntaxException, ClientProtocolException, IOException
    {
        HttpResponse response = null;
        CouchDBDesignDocument designDocument = new CouchDBDesignDocument();
        Map<String, MapReduce> views = new HashMap<String, CouchDBDesignDocument.MapReduce>();
        designDocument.setLanguage("javascript");
        String id = CouchDBConstants.DESIGN + CouchDBConstants.URL_SAPRATOR + tableName;
        for (String columnName : columns)
        {
            MapReduce mapr = new MapReduce();
            mapr.setMap("function(doc){" + CouchDBConstants.LINE_SEP + " if(doc." + columnName + "){"
                    + CouchDBConstants.LINE_SEP + " emit(doc." + columnName + ", doc); " + CouchDBConstants.LINE_SEP
                    + " } " + CouchDBConstants.LINE_SEP + " }");
            views.put(columnName, mapr);
        }
        designDocument.setViews(views);
        URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                CouchDBConstants.URL_SAPRATOR + databaseName + CouchDBConstants.URL_SAPRATOR + id, null, null);
        HttpPut put = new HttpPut(uri);

        JsonObject jsonObject = new Gson().toJsonTree(designDocument).getAsJsonObject();
        jsonObject.addProperty("_id", id);

        StringEntity entity = new StringEntity(jsonObject.toString());
        put.setEntity(entity);
        try
        {
            response = httpClient.execute(httpHost, put, CouchDBUtils.getContext(httpHost));
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }
}