/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.couchdb.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.HttpContext;

import com.google.gson.Gson;
import com.impetus.client.couchdb.CouchDBConstants;
import com.impetus.client.couchdb.CouchDBUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class CouchDBTestUtils.
 * 
 * @author Kuldeep Mishra
 */
public class CouchDBTestUtils
{

    /**
     * Initiate http client.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     * @param persistenceUnit
     *            the persistence unit
     * @return the http client
     */
    public static HttpClient initiateHttpClient(final KunderaMetadata kunderaMetadata, String persistenceUnit)
    {
        PersistenceUnitMetadata pumMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                persistenceUnit);

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

    /**
     * Creates the database.
     * 
     * @param databaseName
     *            the database name
     * @param client
     *            the client
     * @param httpHost
     *            the http host
     */
    public static void createDatabase(String databaseName, HttpClient client, HttpHost httpHost)
    {
        HttpResponse response = null;
        try
        {
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + databaseName, null, null);
            HttpPut put = new HttpPut(uri);
            response = client.execute(httpHost, put, CouchDBUtils.getContext(httpHost));
        }
        catch (URISyntaxException e)
        {

        }
        catch (ClientProtocolException e)
        {

        }
        catch (IOException e)
        {

        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }

    /**
     * Drop database.
     * 
     * @param databaseName
     *            the database name
     * @param client
     *            the client
     * @param httpHost
     *            the http host
     */
    public static void dropDatabase(String databaseName, HttpClient client, HttpHost httpHost)
    {
        HttpResponse response = null;
        try
        {
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + databaseName, null, null);
            HttpDelete delete = new HttpDelete(uri);

            response = client.execute(httpHost, delete, CouchDBUtils.getContext(httpHost));
        }
        catch (URISyntaxException e)
        {

        }
        catch (ClientProtocolException e)
        {

        }
        catch (IOException e)
        {

        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }

    /**
     * Creates the views.
     * 
     * @param columns
     *            the columns
     * @param tableName
     *            the table name
     * @param httpHost
     *            the http host
     * @param databaseName
     *            the database name
     * @param httpClient
     *            the http client
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws ClientProtocolException
     *             the client protocol exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void createViews(String[] columns, String tableName, HttpHost httpHost, String databaseName,
            HttpClient httpClient) throws URISyntaxException, ClientProtocolException, IOException
    {
        Gson gson = new Gson();
        for (String columnName : columns)
        {
            List<String> columnList = new ArrayList<String>();
            columnList.add(columnName);
            CouchDBUtils.createDesignDocumentIfNotExist(httpClient, httpHost, gson, tableName, databaseName,
                    columnName, columnList);
        }
    }
}