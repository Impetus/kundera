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
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.OperationNotSupportedException;
import javax.persistence.metamodel.EmbeddableType;

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
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * CouchDB client.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBClient extends ClientBase implements Client<CouchDBQuery>, Batcher, ClientPropertiesSetter
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(CouchDBClient.class);

    /** The gson. */
    private Gson gson = new Gson();

    /** The http client. */
    private HttpClient httpClient;

    /** The http host. */
    private HttpHost httpHost;

    /** The nodes. */
    private List<Node> nodes = new ArrayList<Node>();

    /** The batch size. */
    private int batchSize;

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new couch db client.
     * 
     * @param factory
     *            the factory
     * @param client
     *            the client
     * @param httpHost
     *            the http host
     * @param reader
     *            the reader
     * @param persistenceUnit
     *            the persistence unit
     * @param externalProperties
     *            the external properties
     * @param clientMetadata
     *            the client metadata
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchDBClient(CouchDBClientFactory factory, HttpClient client, HttpHost httpHost, EntityReader reader,
            String persistenceUnit, Map<String, Object> externalProperties, ClientMetadata clientMetadata,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.httpClient = client;
        this.httpHost = httpHost;
        this.reader = reader;
        this.indexManager = factory.getIndexManager();
        this.clientMetadata = clientMetadata;
        this.setBatchSize(persistenceUnit, externalProperties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public Object find(Class entityClass, Object key)
    {
        HttpResponse response = null;
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        try
        {
            if (key instanceof JsonElement)
            {
                key = ((JsonElement) key).getAsString();
            }
            String _id = null;
            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                Field field = (Field) entityMetadata.getIdAttribute().getJavaMember();
                EmbeddableType embeddableType = metaModel.embeddable(entityMetadata.getIdAttribute()
                        .getBindableJavaType());
                _id = CouchDBObjectMapper.get_Id(field, key, embeddableType, entityMetadata.getTableName());
            }
            else
            {
                _id = entityMetadata.getTableName() + PropertyAccessorHelper.getString(key);
            }

            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SEPARATOR + _id, null, null);
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
                    entityMetadata.getRelationNames(), kunderaMetadata);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        List results = new ArrayList();
        for (Object key : keys)
        {
            Object object = find(entityClass, key);
            if (object != null)
            {
                results.add(object);
            }
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        externalProperties = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        HttpResponse response = null;
        try
        {
            EntityMetadata entityMetadata = KunderaMetadataManager
                    .getEntityMetadata(kunderaMetadata, entity.getClass());
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());
            String _id = null;
            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                Field field = (Field) entityMetadata.getIdAttribute().getJavaMember();
                EmbeddableType embeddableType = metaModel.embeddable(entityMetadata.getIdAttribute()
                        .getBindableJavaType());
                _id = CouchDBObjectMapper.get_Id(field, pKey, embeddableType, entityMetadata.getTableName());
            }
            else
            {
                _id = entityMetadata.getTableName() + PropertyAccessorHelper.getString(pKey);
            }
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SEPARATOR + _id, null, null);
            HttpGet get = new HttpGet(uri);
            get.addHeader("Accept", "application/json");
            response = httpClient.execute(get);

            Reader reader = new InputStreamReader(response.getEntity().getContent());
            JsonObject json = gson.fromJson(reader, JsonObject.class);
            closeContent(response);
            if (!(response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND))
            {
                onDelete(entityMetadata.getSchema(), _id, response, json);
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
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
                            CouchDBConstants.URL_SEPARATOR + joinTableData.getSchemaName().toLowerCase()
                                    + CouchDBConstants.URL_SEPARATOR + id, null, null);

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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
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
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        List foreignKeys = new ArrayList();
        HttpResponse response = null;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        try
        {
            String q = "key=" + CouchDBUtils.appendQuotes(columnValue);
            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        URI uri = null;
        HttpResponse response = null;
        try
        {
            String q = "key=" + CouchDBUtils.appendQuotes(columnValue);
            uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR
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

    /**
     * On delete.
     * 
     * @param schemaName
     *            the schema name
     * @param pKey
     *            the key
     * @param response
     *            the response
     * @param jsonObject
     *            the json object
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws ClientProtocolException
     *             the client protocol exception
     */
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
                CouchDBConstants.URL_SEPARATOR + schemaName.toLowerCase() + CouchDBConstants.URL_SEPARATOR + pKey, q,
                null);

        HttpDelete delete = new HttpDelete(uri);

        response = httpClient.execute(delete);
        closeContent(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        Object[] ids = findIdsByColumn(m.getSchema(), m.getTableName(),
                ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName(), colName, colValue, m.getEntityClazz());
        List<Object> resultSet = new ArrayList<Object>();
        if (ids != null)
        {
            for (Object id : new HashSet(Arrays.asList(ids)))
            {
                Object object = find(entityClazz, id);
                if (object != null)
                {
                    resultSet.add(object);
                }
            }
        }
        return resultSet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<CouchDBQuery> getQueryImplementor()
    {
        return CouchDBQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        HttpResponse response = null;
        try
        {
            JsonObject object = CouchDBObjectMapper.getJsonOfEntity(entityMetadata, entity, id, rlHolders,
                    kunderaMetadata);

            String _id = object.get("_id").getAsString();

            URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(),
                    CouchDBConstants.URL_SEPARATOR + entityMetadata.getSchema().toLowerCase()
                            + CouchDBConstants.URL_SEPARATOR + _id, null, null);

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

    /**
     * Close content.
     * 
     * @param response
     *            the response
     */
    private void closeContent(HttpResponse response)
    {
        CouchDBUtils.closeContent(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        new CouchDbDBClientProperties().populateClientProperties(client, properties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera
     * .graph.Node)
     */
    @Override
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }
        onBatchLimit();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
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
                        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                node.getDataClass());
                        databaseName = metadata.getSchema();
                        JsonObject asJsonObject = CouchDBObjectMapper.getJsonOfEntity(metadata, node.getData(),
                                node.getEntityId(), getRelationHolders(node), kunderaMetadata);
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
                            CouchDBConstants.URL_SEPARATOR + databaseName.toLowerCase()
                                    + CouchDBConstants.URL_SEPARATOR + "_bulk_docs", null, null);

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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#clear()
     */
    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
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
     * Creates the and execute query.
     * 
     * @param interpreter
     *            the interpreter
     * @return the list
     */
    List createAndExecuteQuery(CouchDBQueryInterpreter interpreter)
    {
        EntityMetadata m = interpreter.getMetadata();
        List results = new ArrayList();
        try
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            StringBuilder q = new StringBuilder();
            String _id = CouchDBConstants.URL_SEPARATOR + m.getSchema().toLowerCase() + CouchDBConstants.URL_SEPARATOR
                    + CouchDBConstants.DESIGN + m.getTableName() + CouchDBConstants.VIEW;
            if ((interpreter.isIdQuery() && !interpreter.isRangeQuery() && interpreter.getOperator() == null)
                    || interpreter.isQueryOnCompositeKey())
            {
                Object object = null;
                if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
                {
                    EmbeddableType embeddableType = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
                    if (KunderaCoreUtils.countNonSyntheticFields(m.getIdAttribute().getBindableJavaType()) == interpreter
                            .getKeyValues().size())
                    {
                        Object key = CouchDBObjectMapper.getObjectFromJson(gson.toJsonTree(interpreter.getKeyValues())
                                .getAsJsonObject(), m.getIdAttribute().getBindableJavaType(), embeddableType
                                .getAttributes());
                        object = find(m.getEntityClazz(), key);
                        if (object != null)
                        {
                            results.add(object);
                        }
                        return results;
                    }
                    else if (m.getIdAttribute().getName().equals(interpreter.getKeyName())
                            && interpreter.getKeyValues().size() == 1)
                    {
                        object = find(m.getEntityClazz(), interpreter.getKeyValue());
                        if (object != null)
                        {
                            results.add(object);
                        }
                        return results;
                    }
                    else
                    {
                        log.error("There should be each and every field of composite key.");
                        throw new QueryHandlerException("There should be each and every field of composite key.");
                    }
                }
                object = find(m.getEntityClazz(), interpreter.getKeyValue());
                if (object != null)
                {
                    results.add(object);
                }
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
            executeQueryAndGetResults(q, _id, m, results, interpreter);
        }
        catch (Exception e)
        {
            log.error("Error while executing query, Caused by {}.", e);
            throw new KunderaException(e);
        }
        return results;
    }

    /**
     * Execute query and get results.
     * 
     * @param q
     *            the q
     * @param _id
     *            the _id
     * @param m
     *            the m
     * @param results
     *            the results
     * @param interpreter
     *            the interpreter
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws ClientProtocolException
     *             the client protocol exception
     */
    void executeQueryAndGetResults(StringBuilder q, String _id, EntityMetadata m, List results,
            CouchDBQueryInterpreter interpreter) throws IOException, URISyntaxException, ClientProtocolException
    {
        HttpResponse response = null;
        try
        {
            response = getResponse(q, _id);
            JsonArray array = getJsonFromResponse(response);
            if (interpreter != null && interpreter.isAggregation())
            {
                setAggregatedValuesInResult(results, interpreter, array);
            }
            else if (interpreter != null && interpreter.getColumns() != null && interpreter.getColumns().length != 0)
            {
                setSpecificFieldsInResult(interpreter.getColumnsToOutput(), m, results, array);
            }
            else
            {
                setEntitiesInResult(m, results, array);
            }
        }
        finally
        {
            CouchDBUtils.closeContent(response);
        }
    }

    /**
     * Sets the aggregated values in result.
     * 
     * @param results
     *            the results
     * @param interpreter
     *            the interpreter
     * @param array
     *            the array
     */
    private void setAggregatedValuesInResult(List results, CouchDBQueryInterpreter interpreter, JsonArray array)
    {
        for (JsonElement json : array)
        {
            JsonElement value = json.getAsJsonObject().get("value");
            if (interpreter.getAggregationType().equals(CouchDBConstants.COUNT))
                results.add(value.getAsInt());
            else
                results.add(value.getAsDouble());
        }
    }

    /**
     * Sets the specific fields in result.
     * 
     * @param columnsToOutput
     *            the columns to output
     * @param m
     *            the m
     * @param results
     *            the results
     * @param array
     *            the array
     */
    private void setSpecificFieldsInResult(List<Map<String, Object>> columnsToOutput, EntityMetadata m, List results,
            JsonArray array)
    {
        Map<String, Class> colNameToJavaType = new HashMap<String, Class>();
        for (Map<String, Object> map : columnsToOutput)
        {
            colNameToJavaType.put((String) map.get(Constants.COL_NAME), (Class) map.get(Constants.FIELD_CLAZZ));
        }
        Map<String, List<Object>> outputResults = new HashMap<String, List<Object>>();
        for (JsonElement element : array)
        {
            String column = element.getAsJsonObject().get("key").getAsString();
            String value = element.getAsJsonObject().get("value").getAsString();
            String id = element.getAsJsonObject().get("id").getAsString();
            Object obj = PropertyAccessorHelper.fromSourceToTargetClass(colNameToJavaType.get(column), String.class,
                    value);
            if (colNameToJavaType.size() > 1)
            {
                if (!outputResults.containsKey(id))
                {
                    outputResults.put(id, new ArrayList<Object>());
                }
                outputResults.get(id).add(obj);
            }
            else
            {
                results.add(obj);
            }
        }
        if (colNameToJavaType.size() > 1)
        {
            results.addAll(outputResults.values());
        }
    }

    /**
     * Sets the entities in result.
     * 
     * @param m
     *            the m
     * @param results
     *            the results
     * @param array
     *            the array
     */
    private void setEntitiesInResult(EntityMetadata m, List results, JsonArray array)
    {
        for (JsonElement element : array)
        {

            String id = element.getAsJsonObject().get("value").getAsJsonObject()
                    .get(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()).getAsString();
            Object entityFromJson = CouchDBObjectMapper.getEntityFromJson(m.getEntityClazz(), m, element
                    .getAsJsonObject().get("value").getAsJsonObject(), m.getRelationNames(), kunderaMetadata);
            if (entityFromJson != null
                    && (m.getTableName().concat(id)).equals(element.getAsJsonObject().get("id").getAsString()))
            {
                results.add(entityFromJson);
            }

        }
    }

    /**
     * Gets the json from response.
     * 
     * @param response
     *            the response
     * @return the json from response
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private JsonArray getJsonFromResponse(HttpResponse response) throws IOException
    {
        InputStream content = response.getEntity().getContent();
        Reader reader = new InputStreamReader(content);
        JsonObject json = gson.fromJson(reader, JsonObject.class);
        JsonElement jsonElement = json.get("rows");
        return jsonElement == null ? null : jsonElement.getAsJsonArray();
    }

    /**
     * Gets the response.
     * 
     * @param q
     *            the q
     * @param _id
     *            the _id
     * @return the response
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws ClientProtocolException
     *             the client protocol exception
     */
    private HttpResponse getResponse(StringBuilder q, String _id) throws URISyntaxException, IOException,
            ClientProtocolException
    {
        URI uri = new URI(CouchDBConstants.PROTOCOL, null, httpHost.getHostName(), httpHost.getPort(), _id,
                q.toString(), null);
        HttpGet get = new HttpGet(uri);
        get.addHeader("Accept", "application/json");
        return httpClient.execute(httpHost, get, CouchDBUtils.getContext(httpHost));
    }

    /**
     * Creates the query.
     * 
     * @param interpreter
     *            the interpreter
     * @param m
     *            the m
     * @param q
     *            the q
     * @param _id
     *            the _id
     * @return the string
     * @throws URISyntaxException
     *             the URI syntax exception
     * @throws UnsupportedEncodingException
     *             the unsupported encoding exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws ClientProtocolException
     *             the client protocol exception
     */
    String createQuery(CouchDBQueryInterpreter interpreter, EntityMetadata m, StringBuilder q, String _id)
            throws URISyntaxException, UnsupportedEncodingException, IOException, ClientProtocolException
    {
        if (interpreter.isAggregation())
        {
            _id = CouchDBConstants.URL_SEPARATOR + m.getSchema().toLowerCase() + CouchDBConstants.URL_SEPARATOR
                    + CouchDBConstants.DESIGN + CouchDBConstants.AGGREGATIONS + CouchDBConstants.VIEW
                    + interpreter.getAggregationType();
            if (interpreter.getAggregationColumn() != null)
            {
                q.append("key=");
                q.append("\"" + interpreter.getAggregationColumn() + "_" + m.getTableName() + "\"");

            }
            else
            {
                q.append("key=" + "\"" + CouchDBConstants.ALL + "_" + m.getTableName() + "\"");
            }
            q.append("&group=true");
        }
        else if (!interpreter.isRangeQuery() && interpreter.getOperator() != null
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
        else if (interpreter.getColumns() != null && interpreter.getColumns().length != 0)
        {
            _id += CouchDBConstants.FIELDS;
            q.append("keys=[");
            for (String column : interpreter.getColumns())
            {
                q.append("\"" + column + "\",");
            }
            q.setCharAt(q.length() - 1, ']');
        }
        else
        {
            _id = _id + "all";
        }

        return _id;
    }

    /**
     * Sets the batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
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
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    /**
     * Sets the batch size.
     * 
     * @param batch_Size
     *            the new batch size
     */
    void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(CouchDBIdGenerator.class);
    }
}