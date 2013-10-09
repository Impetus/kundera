package com.impetus.client.couchdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

public class CouchDBClient extends ClientBase implements Client<CouchDBQuery>, Batcher, ClientPropertiesSetter,
        AutoGenerator
{
    private static final String SAPRATOR = "/";

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(CouchDBClient.class);

    private static final String _PROTOCOL = "http";

    private Gson gson = new Gson();

    private HttpClient httpClient;

    private String persistenceUnit;

    private Map<String, Object> externalProperty;

    private HttpHost httpHost;

    public CouchDBClient(HttpClient client, HttpHost httpHost, EntityReader reader, String persistenceUnit,
            Map<String, Object> externalProperties, ClientMetadata clientMetadata)
    {
        this.httpClient = client;
        this.httpHost = httpHost;
        this.persistenceUnit = persistenceUnit;
        this.externalProperty = externalProperties;
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        URI uri = null;
        HttpResponse response = null;
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        try
        {
            uri = new URI(_PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), SAPRATOR
                    + entityMetadata.getSchema() + SAPRATOR + key, null, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);
            return gson.fromJson(reader, entityClass);
        }
        catch (Exception e)
        {
            log.error("Error while deleting object, Caused by: .", e);
            throw new KunderaException(e);
        }
        finally
        {
            if (response != null)
            {
                try
                {
                    response.getEntity().getContent().close();
                }
                catch (Exception e)
                {
                    log.error("Error while deleting object, Caused by: .", e);
                    throw new KunderaException(e);
                }
            }
        }
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        List results = new ArrayList();
        for (Object key : keys)
        {
            results.add(find(entityClass, key));
        }
        return results;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        HttpResponse response = null;
        URI uri = null;
        try
        {
            try
            {

                EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
                HttpGet get;
                Reader reader;
                uri = new URI(_PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), SAPRATOR
                        + entityMetadata.getSchema() + SAPRATOR + pKey, null, null);
                get = new HttpGet(uri);
                get.addHeader("Accept", "application/json");
                response = httpClient.execute(get);

                reader = new InputStreamReader(response.getEntity().getContent());
                JsonObject json = gson.fromJson(reader, JsonObject.class);

                JsonElement rev = json.get("_rev");

                StringBuilder builder = new StringBuilder();
                builder.append("rev=");
                builder.append(rev.getAsString());
                String q = builder.toString();

                uri = new URI(_PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), SAPRATOR + entityMetadata.getSchema()+SAPRATOR
                        + pKey, q, null);
            }
            finally
            {
                response.getEntity().getContent().close();
            }
            HttpDelete delete = new HttpDelete(uri);

            response = httpClient.execute(delete);
        }
        catch (Exception e)
        {
            log.error("Error while deleting object, Caused by: .", e);
            throw new KunderaException(e);
        }
        finally
        {
            if (response != null)
            {
                try
                {
                    response.getEntity().getContent().close();
                }
                catch (Exception e)
                {
                    log.error("Error while deleting object, Caused by: .", e);
                    throw new KunderaException(e);
                }
            }
        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<CouchDBQuery> getQueryImplementor()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        JsonObject object = gson.toJsonTree(entity).getAsJsonObject();
        URI uri = null;
        try
        {
            uri = new URI(_PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), SAPRATOR
                    + entityMetadata.getSchema() + SAPRATOR + id, null, null);
        }
        catch (URISyntaxException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        HttpPut put = new HttpPut(uri);

        StringEntity stringEntity = null;
        try
        {
            stringEntity = new StringEntity(object.toString(), Constants.CHARSET_UTF8);
        }
        catch (UnsupportedEncodingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        stringEntity.setContentType("application/json");
        put.setEntity(stringEntity);

        try
        {
            HttpResponse response = httpClient.execute(httpHost, put, getContext());
        }
        catch (ClientProtocolException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Override
    public Object generate()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void addBatch(Node node)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public int executeBatch()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getBatchSize()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void clear()
    {
        // TODO Auto-generated method stub

    }

    private HttpContext getContext()
    {
        AuthCache authCache = new BasicAuthCache();
        authCache.put(httpHost, new BasicScheme());

        HttpContext context = new BasicHttpContext();
        context.setAttribute(ClientContext.AUTH_CACHE, authCache);
        return context;
    }
}
