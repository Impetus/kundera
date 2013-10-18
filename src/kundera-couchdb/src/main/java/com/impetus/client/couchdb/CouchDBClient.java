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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.naming.OperationNotSupportedException;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.impetus.client.couchdb.CouchDBDesignDocument.MapReduce;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * CouchDB client.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBClient extends ClientBase implements Client<CouchDBQuery>, Batcher, ClientPropertiesSetter,
        AutoGenerator
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(CouchDBClient.class);

    private Gson gson = new Gson();

    private HttpClient httpClient;

    private Map<String, Object> externalProperty;

    private HttpHost httpHost;

    private List<Node> nodes = new ArrayList<Node>();

    private int batchSize;

    /** The reader. */
    private EntityReader reader;

    public CouchDBClient(HttpClient client, HttpHost httpHost, EntityReader reader, String persistenceUnit,
            Map<String, Object> externalProperties, ClientMetadata clientMetadata)
    {
        this.httpClient = client;
        this.httpHost = httpHost;
        this.externalProperty = externalProperties;
        this.reader = reader;
        this.clientMetadata = clientMetadata;
        this.setBatchSize(persistenceUnit, externalProperty);
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        HttpResponse response = null;
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        try
        {
            if (key instanceof JsonElement)
            {
                key = ((JsonElement) key).getAsString();
            }

            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SAPRATOR + entityMetadata.getTableName() + key, null, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);

            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);

            // Check for deleted object. if object is deleted then return null.
            if (jsonObject.get(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()) == null)
            {
                return null;
            }

            return CouchDBObjectMapper.getEntityFromJson(entityClass, entityMetadata, jsonObject,
                    entityMetadata.getRelationNames());
        }
        catch (Exception e)
        {
            log.error("Error while finding object by key {}, Caused by {}.", key, e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
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
        return null;
    }

    @Override
    public void close()
    {
        externalProperty = null;
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        HttpResponse response = null;
        URI uri = null;
        try
        {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
            HttpGet get;
            Reader reader;
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SAPRATOR + entityMetadata.getTableName() + pKey, null, null);
            get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            reader = new InputStreamReader(response.getEntity().getContent());
            JsonObject json = gson.fromJson(reader, JsonObject.class);
            closeContent(response);
            if (!(response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND))
            {
                onDelete(entityMetadata.getSchema(), entityMetadata.getTableName() + pKey, response, json);
            }
        }
        catch (Exception e)
        {
            log.error("Error while deleting object, Caused by {}.", e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);
            Object joinColumnValue = key;

            for (Object childId : values)
            {
                Map<String, Object> obj = new HashMap<String, Object>();
                String id = joinTableName + "#" + joinColumnValue.toString() + "#" + childId;
                obj.put("_id", id);
                obj.put(joinColumnName, joinColumnValue);
                obj.put(invJoinColumnName, childId);
                JsonObject object = gson.toJsonTree(obj).getAsJsonObject();
                HttpResponse response = null;
                try
                {
                    URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                            CouchDBConstants.URL_SAPRATOR + joinTableData.getSchemaName().toLowerCase()
                                    + CouchDBConstants.URL_SAPRATOR + id, null, null);

                    HttpPut put = new HttpPut(uri);
                    StringEntity stringEntity = null;
                    stringEntity = new StringEntity(object.toString(), Constants.CHARSET_UTF8);
                    stringEntity.setContentType("application/json");
                    put.setEntity(stringEntity);

                    response = httpClient.execute(httpHost, put, CouchDBUtils.getContext(httpHost));
                }
                catch (Exception e)
                {
                    log.error("Error while persisting joinTable data, coused by {}.", e);
                    throw new KunderaException(e);
                }
                finally
                {
                    closeContent(response);
                }
            }
        }
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName,
            String inverseJoinColumnName, Object pKeyColumnValue, Class columnJavaType)
    {
        List<E> foreignKeys = new ArrayList<E>();

        URI uri = null;
        HttpResponse response = null;
        try
        {
            String q = "key=" + CouchDBUtils.appendQuotes(pKeyColumnValue);
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SAPRATOR
                            + CouchDBConstants.DESIGN + tableName + CouchDBConstants.VIEW + pKeyColumnName, q, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);
            JsonObject json = gson.fromJson(reader, JsonObject.class);

            JsonElement jsonElement = json.get("rows");

            if (jsonElement == null)
            {
                return foreignKeys;
            }

            JsonArray array = jsonElement.getAsJsonArray();
            for (JsonElement element : array)
            {
                JsonElement value = element.getAsJsonObject().get("value").getAsJsonObject().get(inverseJoinColumnName);
                if (value != null)
                {
                    foreignKeys.add((E) PropertyAccessorHelper.fromSourceToTargetClass(columnJavaType, String.class,
                            value.getAsString()));
                }
            }
        }
        catch (Exception e)
        {
            log.error("Error while fetching column by id {}, Caused by {}.", pKeyColumnValue, e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
        }
        return foreignKeys;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        List foreignKeys = new ArrayList();
        HttpResponse response = null;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);
        try
        {
            String q = "key=" + CouchDBUtils.appendQuotes(columnValue);
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SAPRATOR
                            + CouchDBConstants.DESIGN + tableName + CouchDBConstants.VIEW + columnName, q, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);
            JsonObject json = gson.fromJson(reader, JsonObject.class);

            JsonElement jsonElement = json.get("rows");
            if (jsonElement == null)
            {
                return foreignKeys.toArray();
            }
            JsonArray array = jsonElement.getAsJsonArray();
            for (JsonElement element : array)
            {
                JsonElement value = element.getAsJsonObject().get("value").getAsJsonObject().get(pKeyName);
                if (value != null)
                {
                    foreignKeys.add(PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute()
                            .getBindableJavaType(), String.class, value.getAsString()));
                }
            }
        }
        catch (Exception e)
        {
            log.error("Error while fetching ids for column where column name is" + columnName
                    + " and column value is {} , Caused by {}.", columnValue, e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
        }
        return foreignKeys.toArray();
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        URI uri = null;
        HttpResponse response = null;
        try
        {
            String q = "key=" + CouchDBUtils.appendQuotes(columnValue);
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SAPRATOR
                            + CouchDBConstants.DESIGN + tableName + CouchDBConstants.VIEW + columnName, q, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);

            JsonObject json = gson.fromJson(reader, JsonObject.class);

            JsonElement jsonElement = json.get("rows");

            closeContent(response);

            JsonArray array = jsonElement.getAsJsonArray();
            for (JsonElement element : array)
            {
                JsonObject jsonObject = element.getAsJsonObject().get("value").getAsJsonObject();

                JsonElement pkey = jsonObject.get("_id");

                onDelete(schemaName, pkey.getAsString(), response, jsonObject);
            }
        }
        catch (Exception e)
        {
            log.error("Error while deleting row by column where column name is " + columnName
                    + " and column value is {}, Caused by {}.", columnValue, e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
        }
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

        uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                CouchDBConstants.URL_SAPRATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SAPRATOR + pKey, q,
                null);

        HttpDelete delete = new HttpDelete(uri);

        response = httpClient.execute(delete);
        closeContent(response);
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);
        Object[] ids = findIdsByColumn(m.getSchema(), m.getTableName(),
                ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName(), colName, colValue, m.getEntityClazz());
        List<Object> resultSet = new ArrayList<Object>();
        if (ids != null)
        {
            for (Object id : new HashSet(Arrays.asList(ids)))
            {
                resultSet.add(find(entityClazz, id));
            }
        }
        return resultSet;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<CouchDBQuery> getQueryImplementor()
    {
        return CouchDBQuery.class;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        HttpResponse response = null;
        try
        {
            JsonObject object = CouchDBObjectMapper.getJsonOfEntity(entityMetadata, entity, id, rlHolders);
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SAPRATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SAPRATOR + entityMetadata.getTableName() + id, null, null);

            HttpPut put = new HttpPut(uri);

            StringEntity stringEntity = null;

            if (isUpdate)
            {
                HttpGet get = new HttpGet(uri);
                get.addHeader("Accept", "application/json");
                response = httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));

                try
                {
                    InputStream content = response.getEntity().getContent();
                    Reader reader = new InputStreamReader(content);
                    JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
                    JsonElement rev = jsonObject.get("_rev");
                    object.add("_rev", rev);
                }
                finally
                {
                    closeContent(response);
                }
            }
            object.addProperty("_id", entityMetadata.getTableName() + id);
            stringEntity = new StringEntity(object.toString(), Constants.CHARSET_UTF8);
            stringEntity.setContentType("application/json");
            put.setEntity(stringEntity);

            response = httpClient.execute(httpHost, put, CouchDBUtils.getContext(httpHost));

        }
        catch (Exception e)
        {
            log.error("Error while persisting entity with id {}, caused by {}. ", id, e);
            throw new KunderaException(e);
        }
        finally
        {
            closeContent(response);
        }
    }

    private void closeContent(HttpResponse response)
    {
        CouchDBUtils.closeContent(response);
    }

    @Override
    public Object generate()
    {
        // Need to generate using couchdb.
        return UUID.randomUUID().toString();
    }

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        new CouchDbDBClientProperties().populateClientProperties(client, properties);
    }

    @Override
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }
        onBatchLimit();

    }

    @Override
    public int executeBatch()
    {
        List<JsonObject> objectsToPersist = new ArrayList<JsonObject>();
        HttpResponse response = null;
        String databaseName = null;
        boolean isbulk = false;
        try
        {
            for (Node node : nodes)
            {
                if (node.isDirty())
                {
                    node.handlePreEvent();
                    // delete can not be executed in batch
                    if (node.isInState(RemovedState.class))
                    {
                        delete(node.getData(), node.getEntityId());
                    }
                    else
                    {
                        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());
                        databaseName = metadata.getSchema();
                        JsonObject asJsonObject = CouchDBObjectMapper.getJsonOfEntity(metadata, node.getData(),
                                node.getEntityId(), getRelationHolders(node));
                        objectsToPersist.add(asJsonObject);
                        isbulk = true;
                    }
                    node.handlePostEvent();
                }
            }

            if (isbulk)
            {
                try
                {
                    URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                            CouchDBConstants.URL_SAPRATOR + databaseName.toLowerCase() + CouchDBConstants.URL_SAPRATOR
                                    + "_bulk_docs", null, null);

                    HttpPost post = new HttpPost(uri);
                    String object = String.format("{%s%s}", "\"all_or_nothing\": true,",
                            "\"docs\": " + gson.toJson(objectsToPersist));
                    StringEntity entity = new StringEntity(object, "UTF-8");
                    entity.setContentType("application/json");
                    post.setEntity(entity);
                    response = httpClient.execute(httpHost, post, CouchDBUtils.getContext(httpHost));
                }
                catch (Exception e)
                {
                    log.error("Error while executing batch, caused by {}. ", e);
                    throw new KunderaException("Error while executing batch. caused by :" + e);
                }
            }
        }
        catch (OperationNotSupportedException e)
        {
            throw new KunderaException(e.getMessage());
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }

        return nodes.size();
    }

    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = null;
            nodes = new ArrayList<Node>();
        }

    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit()
    {
        if (batchSize > 0 && batchSize == nodes.size())
        {
            executeBatch();
            nodes.clear();
        }
    }

    /**
     * 
     * @param interpreter
     * @return
     */
    List createAndExecuteQuery(CouchDBQueryInterpreter interpreter)
    {
        EntityMetadata m = interpreter.getMetadata();
        List results = new ArrayList();
        try
        {
            StringBuilder q = new StringBuilder();
            String _id = CouchDBConstants.URL_SAPRATOR + m.getSchema().toLowerCase() + CouchDBConstants.URL_SAPRATOR
                    + CouchDBConstants.DESIGN + m.getTableName() + CouchDBConstants.VIEW;
            if (interpreter.isIdQuery() && !interpreter.isRangeQuery() && interpreter.getOperator() == null)
            {
                results.add(find(m.getEntityClazz(), interpreter.getKeyValue()));
                return results;
            }

            // creating query.
            _id = createQuery(interpreter, m, q, _id);

            if (interpreter.getLimit() > 0)
            {
                q.append("&limit=" + interpreter.getLimit());
            }
            if (interpreter.isDescending())
            {
                q.append("&descending=" + false);
            }

            // execute query.
            executeQuery(q, _id, m, results);
        }
        catch (Exception e)
        {
            log.error("Error while executing query, Caused by {}.", e);
            throw new KunderaException(e);
        }
        return results;
    }

    /**
     * 
     * @param q
     * @param _id
     * @param m
     * @param results
     * @throws IOException
     * @throws ClientProtocolException
     * @throws URISyntaxException
     */
    void executeQuery(StringBuilder q, String _id, EntityMetadata m, List results) throws IOException,
            ClientProtocolException, URISyntaxException
    {
        HttpResponse response = null;
        try
        {
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), _id,
                    q.toString(), null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));

            InputStream content = response.getEntity().getContent();
            Reader reader = new InputStreamReader(content);
            JsonObject json = gson.fromJson(reader, JsonObject.class);

            JsonElement jsonElement = json.get("rows");

            if (jsonElement == null)
            {
                return;
            }
            JsonArray array = jsonElement.getAsJsonArray();
            for (JsonElement element : array)
            {
                results.add(CouchDBObjectMapper.getEntityFromJson(m.getEntityClazz(), m,
                        element.getAsJsonObject().get("value").getAsJsonObject(), m.getRelationNames()));
            }
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }

    }

    /**
     * 
     * @param interpreter
     * @param m
     * @param q
     * @param _id
     * @return
     * @throws URISyntaxException
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws ClientProtocolException
     */
    String createQuery(CouchDBQueryInterpreter interpreter, EntityMetadata m, StringBuilder q, String _id)
            throws URISyntaxException, UnsupportedEncodingException, IOException, ClientProtocolException
    {
        if (!interpreter.isRangeQuery() && interpreter.getOperator() != null
                && interpreter.getOperator().equalsIgnoreCase("AND"))
        {
            StringBuilder viewName = new StringBuilder();
            List<String> columns = new ArrayList<String>();
            q.append("key=[");
            for (String columnName : interpreter.getKeyValues().keySet())
            {
                viewName.append(columnName + "AND");
                q.append(CouchDBUtils.appendQuotes(interpreter.getKeyValues().get(columnName)));
                q.append(",");
                columns.add(columnName);
            }
            q.deleteCharAt(q.toString().lastIndexOf(","));
            q.append("]");
            viewName.delete(viewName.toString().lastIndexOf("AND"), viewName.toString().lastIndexOf("AND") + 3);
            _id = _id + viewName.toString();
            CouchDBUtils.createDesignDocumentIfNotExist(httpClient, httpHost, gson, m.getTableName(), m.getSchema(),
                    viewName.toString(), columns);
        }
        else if (interpreter.getKeyValues() != null)
        {
            if (interpreter.getStartKeyValue() != null || interpreter.getEndKeyValue() != null)
            {
                String queryString = null;
                if (interpreter.getStartKeyValue() != null)
                {
                    queryString = "startkey=" + CouchDBUtils.appendQuotes(interpreter.getStartKeyValue());
                    q.append(queryString);
                }
                if (interpreter.getEndKeyValue() != null)
                {
                    if (interpreter.getStartKeyValue() != null)
                    {
                        q.append("&");
                    }
                    queryString = "endkey=" + CouchDBUtils.appendQuotes(interpreter.getEndKeyValue());
                    q.append(queryString);
                    if (interpreter.isIncludeLastKey())
                    {
                        q.append("&inclusive_end=");
                        q.append(true);
                    }
                }
            }
            else if (interpreter.getKeyValue() != null)
            {
                q.append("key=" + CouchDBUtils.appendQuotes(interpreter.getKeyValue()));
            }
            _id = _id + interpreter.getKeyName();
        }
        else
        {
            _id = _id + "all";
        }

        return _id;
    }

    /**
     * @param persistenceUnit
     * @param puProperties
     */
    private void setBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                : null;
        if (batch_Size != null)
        {
            setBatchSize(Integer.valueOf(batch_Size));
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }
}