package com.impetus.client.couchdb;

import java.io.InputStream;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import com.impetus.kundera.KunderaException;

public class CouchDBUtils
{

    public static HttpContext getContext(HttpHost httpHost)
    {
        AuthCache authCache = new BasicAuthCache();
        authCache.put(httpHost, new BasicScheme());

        HttpContext context = new BasicHttpContext();
        context.setAttribute(ClientContext.AUTH_CACHE, authCache);
        return context;
    }

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
}
