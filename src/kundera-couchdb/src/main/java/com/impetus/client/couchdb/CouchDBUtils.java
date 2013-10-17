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

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

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
    /**
     * 
     * @param httpHost
     * @return
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
     * 
     * * @param response
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
     * 
     * @param views
     * @param columnName
     * @param columns
     */
    static void createView(Map<String, MapReduce> views, String columnName, List<String> columns)
    {
        Iterator<String> iterator = columns.iterator();

        MapReduce mapr = new MapReduce();
        StringBuilder mapBuilder = new StringBuilder();
        StringBuilder ifBuilder = new StringBuilder("function(doc){if(");
        StringBuilder emitFunction = new StringBuilder("{emit([");
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
        emitFunction.append("], doc)}}");

        mapBuilder.append(ifBuilder.toString()).append(emitFunction.toString());

        mapr.setMap(mapBuilder.toString());
        views.put(columnName, mapr);
    }
}
