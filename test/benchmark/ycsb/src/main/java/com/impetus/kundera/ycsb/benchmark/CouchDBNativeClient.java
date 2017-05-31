/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.ycsb.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import redis.clients.jedis.Protocol;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.impetus.client.couchdb.CouchDBConstants;
import com.impetus.client.couchdb.CouchDBUtils;
import com.impetus.kundera.Constants;
import com.impetus.kundera.ycsb.utils.CouchDBOperationUtils;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * @author Vivek mishra
 * 
 */
public class CouchDBNativeClient extends DB
{
    private HttpClient httpClient;

    private HttpHost httpHost;

    public static final String HOST_PROPERTY = "hosts";

    public static final String PORT_PROPERTY = "port";

    public static final String INDEX_KEY = "_indices";

    private String database = "schema";

    private Gson gson = new Gson();

    private CouchDBOperationUtils utils = new CouchDBOperationUtils();;

    public void init() throws DBException
    {
        Properties props = getProperties();
        database = props.getProperty("schema", "kundera_native");
        if (httpClient == null || httpHost == null)
        {
            System.out.println("Initailizing ....");
            int port;

            String portString = props.getProperty(PORT_PROPERTY);
            if (portString != null)
            {
                port = Integer.parseInt(portString);
            }
            else
            {
                port = Protocol.DEFAULT_PORT;
            }
            String host = props.getProperty(HOST_PROPERTY);

            httpClient = utils.initiateClient(host, port);
            httpHost = new HttpHost(host, port);
        }
    }

    public void cleanup() throws DBException
    {
        // httpClient.getConnectionManager().shutdown();
        // httpHost = null;
    }

    /*
     * Calculate a hash for a key to store it in an index. The actual return
     * value of this function is not interesting -- it primarily needs to be
     * fast and scattered along the whole space of doubles. In a real world
     * scenario one would probably use the ASCII values of the keys.
     */
    private double hash(String key)
    {
        return key.hashCode();
    }

    // XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        HttpResponse response = null;
        try
        {
            System.out.println("Reading ....");
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + database.toLowerCase() + CouchDBConstants.URL_SEPARATOR + table
                            + key, null, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);

            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND)
            {
                return 1;
            }
            return 0;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // CouchDBUtils.closeContent(response);
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        HttpResponse response = null;
        try
        {
            System.out.println("Inserting ....");
            JsonObject object = new JsonObject();
            for (Map.Entry<String, ByteIterator> entry : values.entrySet())
            {
                object.addProperty(entry.getKey(), entry.getValue().toString());
            }
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + database.toLowerCase() + CouchDBConstants.URL_SEPARATOR + table
                            + key, null, null);

            HttpPut put = new HttpPut(uri);

            StringEntity stringEntity = null;
            object.addProperty("_id", table + key);
            stringEntity = new StringEntity(object.toString(), Constants.CHARSET_UTF8);
            stringEntity.setContentType("application/json");
            put.setEntity(stringEntity);

            response = httpClient.execute(httpHost, put, CouchDBUtils.getContext(httpHost));

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND)
            {
                return 1;
            }
            return 0;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // CouchDBUtils.closeContent(response);
        }
    }

    @Override
    public int delete(String table, String key)
    {

        HttpResponse response = null;
        URI uri = null;
        try
        {
            System.out.println("Deleting ....");
            HttpGet get;
            Reader reader;
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + database.toLowerCase() + CouchDBConstants.URL_SEPARATOR + table
                            + key, null, null);
            get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            reader = new InputStreamReader(response.getEntity().getContent());
            JsonObject json = gson.fromJson(reader, JsonObject.class);
            // CouchDBUtils.closeContent(response);
            if (!(response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND))
            {
                onDelete(database, table + key, response, json);
                return 0;
            }
            return 1;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // CouchDBUtils.closeContent(response);
        }

    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {

        throw new RuntimeException("Operation not supported");
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        throw new RuntimeException("Operation not supported");
    }

    private void onDelete(String schemaName, Object pKey, HttpResponse response, JsonObject jsonObject)
            throws URISyntaxException, IOException, ClientProtocolException
    {
        URI uri;
        String q;
        JsonElement rev = jsonObject.get("_rev");

        StringBuilder builder = new StringBuilder();
        builder.append("rev=");
        builder.append(rev.getAsString());
        q = builder.toString();

        // uri = new URI(CouchDBConstants.PROTOCOL, null,
        // httpHost.getHostName(), httpHost.getPort(),
        // CouchDBConstants.URL_SAPRATOR + schemaName.toLowerCase() +
        // CouchDBConstants.URL_SAPRATOR + pKey, q,
        // null);

        // HttpDelete delete = new HttpDelete(uri);

        // response = httpClient.execute(delete);
        // CouchDBUtils.closeContent(response);
    }

    public static void main(String[] args)
    {

        CouchDBNativeClient cli = new CouchDBNativeClient();

        Properties props = new Properties();

        props.setProperty("hosts", "localhost");
        props.setProperty("port", "5984");
        cli.setProperties(props);

        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = cli.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = cli.read("usertable", "BrianFrankCooper", null, result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet())
        {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);

    }
}