/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.HttpContext;

import com.impetus.client.couchdb.CouchDBConstants;
import com.impetus.client.couchdb.CouchDBUtils;
import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBOperationUtils
{
    private static Logger logger = Logger.getLogger(CouchDBOperationUtils.class);

    private HttpClient httpClient;

    private HttpHost httpHost;

    public void createdatabase(String keyspace, String host, int port) throws URISyntaxException, IOException

    {
        initiateClient(host, port);
        URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                CouchDBConstants.URL_SEPARATOR + keyspace.toLowerCase(), null, null);

        HttpPut put = new HttpPut(uri);
        HttpResponse putRes = null;
        try
        {
            // creating database.
            logger.info("Creating database " + keyspace);
            putRes = httpClient.execute(httpHost, put, CouchDBUtils.getContext(httpHost));
        }
        finally
        {
            CouchDBUtils.closeContent(putRes);
        }
    }

    public void dropDatabase(String keyspace, String host, int port) throws URISyntaxException,
            ClientProtocolException, IOException
    {
        initiateClient(host, port);
        URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                CouchDBConstants.URL_SEPARATOR + keyspace.toLowerCase(), null, null);

        HttpDelete delete = new HttpDelete(uri);
        HttpResponse delReq = null;
        try
        {
            // creating database.
            logger.info("Droping database " + keyspace);
            delReq = httpClient.execute(httpHost, delete, CouchDBUtils.getContext(httpHost));
        }
        finally
        {
            CouchDBUtils.closeContent(delReq);
        }
    }

    public HttpClient initiateClient(String host, int port)
    {
        if (httpClient == null || httpHost == null)
        {
            SchemeSocketFactory ssf = null;
            ssf = PlainSocketFactory.getSocketFactory();
            SchemeRegistry schemeRegistry = new SchemeRegistry();

            schemeRegistry.register(new Scheme("http", port, ssf));
            PoolingClientConnectionManager ccm = new PoolingClientConnectionManager(schemeRegistry);
            
            ccm.setMaxTotal(100);
//            ccm.setDefaultMaxPerRoute(50);
            
            httpClient = new DefaultHttpClient(ccm);
            httpHost = new HttpHost(host, port);

            try
            {
                // Http params
                httpClient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");

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
        }
        return httpClient;
    }
}