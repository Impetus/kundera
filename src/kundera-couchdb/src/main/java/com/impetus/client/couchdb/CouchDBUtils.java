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
package com.impetus.client.couchdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impetus.client.couchdb.CouchDBDesignDocument.MapReduce;
import com.impetus.kundera.KunderaException;

/**
 * Utility class for couchdb.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class CouchDBUtils
{

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(CouchDBUtils.class);

    /**
     * Gets the context.
     * 
     * @param httpHost
     *            the http host
     * @return the context
     */
    public static HttpContext getContext(HttpHost httpHost)
    {
        AuthCache authCache = new BasicAuthCache();
        authCache.put(httpHost, new BasicScheme());

        HttpContext context = new BasicHttpContext();
        context.setAttribute(ClientContext.AUTH_CACHE, authCache);
        return context;
    }

    /**
     * Close content.
     * 
     * @param response
     *            the response
     */
    public static void closeContent(HttpResponse response)
    {
        if (response != null)
        {
            try
            {
                InputStream content = response.getEntity().getContent();
                content.close();
            }
            catch (Exception e)
            {
                throw new KunderaException(e);
            }
        }
    }

    /**
     * Creates the view.
     * 
     * @param views
     *            the views
     * @param columnName
     *            the column name
     * @param columns
     *            the columns
     */
    static void createView(Map<String, MapReduce> views, String columnName, List<String> columns)
    {
        Iterator<String> iterator = columns.iterator();

        MapReduce mapr = new MapReduce();
        StringBuilder mapBuilder = new StringBuilder();
        StringBuilder ifBuilder = new StringBuilder("function(doc){if(");
        StringBuilder emitFunction = new StringBuilder("{emit(");
        if (columns != null && columns.size() > 1)
        {
            emitFunction.append("[");
        }
        while (iterator.hasNext())
        {
            String nextToken = iterator.next();
            ifBuilder.append("doc." + nextToken);
            ifBuilder.append(" && ");
            emitFunction.append("doc." + nextToken);
            emitFunction.append(",");
        }
        ifBuilder.delete(ifBuilder.toString().lastIndexOf(" && "), ifBuilder.toString().lastIndexOf(" && ") + 3);
        emitFunction.deleteCharAt(emitFunction.toString().lastIndexOf(","));

        ifBuilder.append(")");
        if (columns != null && columns.size() > 1)
        {
            emitFunction.append("]");
        }
        emitFunction.append(", doc)}}");

        mapBuilder.append(ifBuilder.toString()).append(emitFunction.toString());

        mapr.setMap(mapBuilder.toString());
        views.put(columnName, mapr);
    }

    /**
     * Append quotes.
     * 
     * @param value
     *            the value
     * @return the object
     */
    static Object appendQuotes(Object value)
    {
        if (value instanceof String || value instanceof Character)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("\"");
            builder.append(value);
            builder.append("\"");
            return builder.toString();
        }
        return value;
    }

    /**
     * Creates the design document if not exist.
     * 
     * @param httpClient
     *            the http client
     * @param httpHost
     *            the http host
     * @param gson
     *            the gson
     * @param tableName
     *            the table name
     * @param schemaName
     *            the schema name
     * @param viewName
     *            the view name
     * @param columns
     *            the columns
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws UnsupportedEncodingException
     *             the unsupported encoding exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws ClientProtocolException
     *             the client protocol exception
     */
    public static void createDesignDocumentIfNotExist(HttpClient httpClient, HttpHost httpHost, Gson gson,
            String tableName, String schemaName, String viewName, List<String> columns) throws URISyntaxException,
            UnsupportedEncodingException, IOException, ClientProtocolException
    {
        URI uri;
        HttpResponse response = null;
        CouchDBDesignDocument designDocument = CouchDBUtils.getDesignDocument(httpClient, httpHost, gson, tableName,
                schemaName);
        Map<String, MapReduce> views = designDocument.getViews();
        if (views == null)
        {
            views = new HashMap<String, MapReduce>();
        }

        if (views.get(viewName.toString()) == null)
        {
            CouchDBUtils.createView(views, viewName, columns);
        }
        String id = CouchDBConstants.DESIGN + tableName;
        if (designDocument.get_rev() == null)
        {
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR + id,
                    null, null);
        }
        else
        {
            StringBuilder builder = new StringBuilder("rev=");
            builder.append(designDocument.get_rev());
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR + id,
                    builder.toString(), null);
        }
        HttpPut put = new HttpPut(uri);

        designDocument.setViews(views);
        String jsonObject = gson.toJson(designDocument);
        StringEntity entity = new StringEntity(jsonObject);
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

    /**
     * Gets the design document.
     * 
     * @param httpClient
     *            the http client
     * @param httpHost
     *            the http host
     * @param gson
     *            the gson
     * @param tableName
     *            the table name
     * @param schemaName
     *            the schema name
     * @return the design document
     */
    private static CouchDBDesignDocument getDesignDocument(HttpClient httpClient, HttpHost httpHost, Gson gson,
            String tableName, String schemaName)
    {
        HttpResponse response = null;
        try
        {
            String id = CouchDBConstants.DESIGN + tableName;
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR + id,
                    null, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);

            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
            return gson.fromJson(jsonObject, CouchDBDesignDocument.class);
        }
        catch (Exception e)
        {
            log.error("Error while fetching design document object, Caused by: .", e);
            throw new KunderaException(e);
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }
}
