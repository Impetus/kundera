/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.redis;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.impetus.client.redis.RedisQueryInterpreter.Clause;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.KunderaTransactionException;
import com.impetus.kundera.persistence.TransactionBinder;
import com.impetus.kundera.persistence.TransactionResource;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.ObjectAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Redis client implementation for REDIS.
 * 
 * @author vivek.mishra
 */
public class RedisClient extends ClientBase implements Client<RedisQuery>, Batcher, ClientPropertiesSetter,
        TransactionBinder
{
    /**
     * Reference to redis client factory.
     */
    RedisClientFactory factory;

    /** The reader. */
    private EntityReader reader;

    /** The settings. */
    private Map<String, Object> settings;

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    /** The resource. */
    private TransactionResource resource;

    /** batch size. */
    private int batchSize;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClient.class);

    /** The Constant COMPOSITE_KEY_SEPERATOR. */
    private static final String COMPOSITE_KEY_SEPERATOR = "\001";

    /** The connection. */
    private Jedis connection;

    /**
     * Instantiates a new redis client.
     * 
     * @param factory
     *            the factory
     * @param puProperties
     *            the pu properties
     * @param persistenceUnit
     *            the persistence unit
     * @param kunderaMetadata
     *            the kundera metadata
     */
    RedisClient(final RedisClientFactory factory, final Map<String, Object> puProperties, final String persistenceUnit,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, puProperties, persistenceUnit);
        this.factory = factory;
        this.reader = new RedisEntityReader(kunderaMetadata);
        this.indexManager = factory.getIndexManager();
        initializeIndexer();
        this.clientMetadata = factory.getClientMetadata();
        setBatchSize(persistenceUnit, factory.getOverridenProperties());
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
        Object connection = getConnection();
        // Create a hashset and populate data into it
        //

        Pipeline pipeLine = null;
        try
        {
            if (isBoundTransaction())
            {
                pipeLine = ((Jedis) connection).pipelined();
                onPersist(entityMetadata, entity, id, rlHolders, pipeLine);
            }
            else
            {
                onPersist(entityMetadata, entity, id, rlHolders, connection);
            }
        }
        finally
        {
            //
            if (pipeLine != null)
            {
                pipeLine.sync(); // send I/O.. as persist call. so no need to
                                 // read
            } // response?

            onCleanup(connection);
        }

    }

    /**
     * Gets the double.
     * 
     * @param valueAsStr
     *            the value as str
     * @return the double
     */
    private double getDouble(String valueAsStr)
    {
        return StringUtils.isNumeric(valueAsStr) ? Double.parseDouble(valueAsStr) : Double
                .parseDouble(((Integer) valueAsStr.hashCode()).toString());
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
        Object result = null;
        Object connection = getConnection();
        try
        {
            result = fetch(entityClass, key, connection, null);
        }
        catch (InstantiationException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
        }
        finally
        {
            onCleanup(connection);
        }

        return result;
    }

    /**
     * Retrieves entity instance of given class,row key and specific fields.
     * 
     * @param clazz
     *            entity class
     * @param key
     *            row key
     * @param connection
     *            connection instance.
     * @param fields
     *            fields.
     * @return entity instance.
     * @throws InstantiationException
     *             throws in case of runtime exception
     * @throws IllegalAccessException
     *             throws in case of runtime exception
     */
    private Object fetch(Class clazz, Object key, Object connection, byte[][] fields) throws InstantiationException,
            IllegalAccessException
    {
        Object result = null;

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        String rowKey = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            if(key instanceof String && ((String) key).indexOf(COMPOSITE_KEY_SEPERATOR)>0){
                rowKey = (String) key;
            }
            else{
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, key);
            }
        }
        else
        {
            ObjectAccessor accessor = new ObjectAccessor();

            rowKey = accessor.toString(key);
        }

        String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);
        KunderaCoreUtils
                .printQuery("Fetch data from " + entityMetadata.getTableName() + " for PK " + rowKey, showQuery);
        try
        {
            Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();

            // IF it is for selective columns
            if (fields != null)
            {
                List<byte[]> fieldValues = null;
                if (resource != null && resource.isActive())
                {
                    Response response = ((Transaction) connection).hmget(getEncodedBytes(hashKey), fields);
                    // ((Transaction) connection).exec();
                    ((RedisTransaction) resource).onExecute(((Transaction) connection));

                    fieldValues = (List<byte[]>) response.get();
                    connection = getConnection();

                }
                else
                {
                    fieldValues = ((Jedis) connection).hmget(getEncodedBytes(hashKey), fields);
                }

                if (fieldValues != null && !fieldValues.isEmpty())
                {
                    for (int i = 0; i < fields.length; i++)
                    {
                        if (fieldValues.get(i) != null)
                        {
                            columns.put(fields[i], fieldValues.get(i));
                        }
                    }
                }
            }
            else
            {
                columns = getColumns(connection, hashKey, columns);
            }
            // Map<byte[], byte[]>
            result = unwrap(entityMetadata, columns, key);
        }
        catch (JedisConnectionException jedex)
        {
            // Jedis is throwing runtime exception in case of no result
            // found!!!!
            return null;
        }

        return result;
    }

    /**
     * Gets the columns.
     * 
     * @param connection
     *            the connection
     * @param hashKey
     *            the hash key
     * @param columns
     *            the columns
     * @return the columns
     */
    private Map<byte[], byte[]> getColumns(Object connection, String hashKey, Map<byte[], byte[]> columns)
    {
        if (resource != null && resource.isActive())
        {
            // Why transaction API returns response in byte[] format/?
            Response response = ((Transaction) connection).hgetAll(getEncodedBytes(hashKey));
            ((RedisTransaction) resource).onExecute(((Transaction) connection));
            // ((Transaction) connection).exec();
            Map<String, String> cols = (Map<String, String>) response.get();
            connection = getConnection();

            if (cols != null)
            {
                for (String name : cols.keySet())
                {
                    columns.put(getEncodedBytes(name), getEncodedBytes(cols.get(name)));
                }
            }
        }
        else
        {
            columns = ((Jedis) connection).hgetAll(getEncodedBytes(hashKey));
        }
        return columns;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        Object connection = getConnection();
        List results = new ArrayList();
        try
        {
            for (Object key : keys)
            {
                Object result = fetch(entityClass, key, connection, null);
                if (result != null)
                {
                    results.add(result);
                }
            }
        }
        catch (InstantiationException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
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
        throw new UnsupportedOperationException("Method not supported!");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        if (settings != null)
        {
            settings.clear();
            settings = null;
        }

        if (connection != null)
        {
            connection.disconnect();
            connection = null;
        }

        reader = null;
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
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        Object connection = getConnection();
        Pipeline pipeLine = null;
        try
        {
            if (isBoundTransaction())
            {
                pipeLine = ((Jedis) connection).pipelined();
                onDelete(entity, pKey, pipeLine);
            }
            else
            {
                onDelete(entity, pKey, connection);
            }
            getIndexManager().remove(metadata, entity, pKey);
        }
        finally
        {
            if (pipeLine != null)
            {
                pipeLine.sync();
            }
            onCleanup(connection);
        }
    }

    /**
     * On delete relation.
     * 
     * @param connection
     *            connection instance.
     * @param entityMetadata
     *            entity metadata.
     * @param rowKey
     *            row key.
     */
    private void deleteRelation(Object connection, EntityMetadata entityMetadata, String rowKey)
    {
        List<String> relations = entityMetadata.getRelationNames();

        if (relations != null)
        {
            for (String relation : relations)
            {
                if (resource != null && resource.isActive())
                {
                    ((Transaction) connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey), relation);

                }
                else
                {
                    ((Pipeline) connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey), relation);

                }
            }

        }
        /*
         * Response<Map<String, String>> fields = null; if (resource != null &&
         * resource.isActive()) { fields = ((Transaction)
         * connection).hgetAll(getHashKey(entityMetadata.getTableName(),
         * rowKey)); if (connection != null) { ((Pipeline) connection).sync(); }
         * for (String field : fields.get().keySet()) { ((Transaction)
         * connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey),
         * field); } } else { fields = ((Pipeline)
         * connection).hgetAll(getHashKey(entityMetadata.getTableName(),
         * rowKey)); if (connection != null) { ((Pipeline) connection).sync(); }
         * for (String field : fields.get().keySet()) { ((Pipeline)
         * connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey),
         * field); } }
         */
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
        String tableName = joinTableData.getJoinTableName();
        String inverseJoinColumn = joinTableData.getInverseJoinColumnName();
        String joinColumn = joinTableData.getJoinColumnName();

        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();
        Object connection = null;
        Pipeline pipeline = null;
        /**
         * Example: join table : PERSON_ADDRESS join column : PERSON_ID (1_p)
         * inverse join column : ADDRESS_ID (1_a) store in REDIS:
         * PERSON_ADDRESS:1_p_1_a PERSON_ID 1_p ADDRESS_ID 1_a
         */
        // String rowKey =
        try
        {
            connection = getConnection();
            if (isBoundTransaction())
            {
                pipeline = ((Jedis) connection).pipelined();
            }
            Set<Object> joinKeys = joinTableRecords.keySet();

            for (Object joinKey : joinKeys)
            {
                String joinKeyAsStr = PropertyAccessorHelper.getString(joinKey);

                Set<Object> inverseKeys = joinTableRecords.get(joinKey);

                for (Object inverseKey : inverseKeys)
                {
                    Map<byte[], byte[]> redisFields = new HashMap<byte[], byte[]>(1);
                    String inverseJoinKeyAsStr = PropertyAccessorHelper.getString(inverseKey);
                    String redisKey = getHashKey(tableName, joinKeyAsStr + "_" + inverseJoinKeyAsStr);
                    redisFields.put(getEncodedBytes(joinColumn), getEncodedBytes(joinKeyAsStr)); // put
                                                                                                 // join
                                                                                                 // column
                                                                                                 // field.
                    redisFields.put(getEncodedBytes(inverseJoinColumn), getEncodedBytes(inverseJoinKeyAsStr)); // put
                                                                                                               // inverse
                                                                                                               // join
                                                                                                               // column
                                                                                                               // field

                    // add to hash table.

                    if (resource != null && resource.isActive())
                    {
                        ((Transaction) connection).hmset(getEncodedBytes(redisKey), redisFields);
                        // add index
                        ((Transaction) connection).zadd(getHashKey(tableName, inverseJoinKeyAsStr),
                                getDouble(inverseJoinKeyAsStr), redisKey);
                        ((Transaction) connection).zadd(getHashKey(tableName, joinKeyAsStr), getDouble(joinKeyAsStr),
                                redisKey);

                    }
                    else
                    {
                        ((Jedis) connection).hmset(getEncodedBytes(redisKey), redisFields);
                        // add index
                        ((Jedis) connection).zadd(getHashKey(tableName, inverseJoinKeyAsStr),
                                getDouble(inverseJoinKeyAsStr), redisKey);
                        ((Jedis) connection).zadd(getHashKey(tableName, joinKeyAsStr), getDouble(joinKeyAsStr),
                                redisKey);

                    }
                    redisFields.clear();
                }

            }
            KunderaCoreUtils.printQuery("Persist Join Table:" + tableName, showQuery);
        }
        finally
        {
            if (pipeline != null)
            {
                pipeline.sync();
            }
            onCleanup(connection);
        }

    }

    /**
     * Returns collection of column values for given join table. TODO: Method is
     * very much tightly coupled with Join table implementation and does not
     * serve purpose as it is meant for.
     * 
     * @param <E>
     *            the element type
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param pKeyColumnName
     *            the key column name
     * @param columnName
     *            the column name
     * @param pKeyColumnValue
     *            the key column value
     * @param columnJavaType
     *            the column java type
     * @return the columns by id
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        Object connection = null;

        List results = new ArrayList();

        try
        {
            connection = getConnection();

            String valueAsStr = PropertyAccessorHelper.getString(pKeyColumnValue);

            Double score = getDouble(valueAsStr);

            Set<String> resultKeys = null;
            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).zrangeByScore(getHashKey(tableName, valueAsStr), score,
                        score);
                // ((Transaction) connection).exec();
                ((RedisTransaction) resource).onExecute(((Transaction) connection));

                // ((Transaction)
                // connection).zrangeByScore(getHashKey(tableName, valueAsStr),
                // score, score);
                resultKeys = (Set<String>) response.get();

            }
            else
            {
                resultKeys = ((Jedis) connection).zrangeByScore(getHashKey(tableName, valueAsStr), score, score);
            }

            results = fetchColumn(columnName, connection, results, resultKeys);

            // return connection.hmget(getEncodedBytes(redisKey),
            // getEncodedBytes(columnName));
            KunderaCoreUtils.printQuery("Get columns by id from:" + tableName + " for column:" + columnName
                    + " where value:" + pKeyColumnValue, showQuery);
            return results;
        }
        finally
        {
            onCleanup(connection);
        }
    }

    /**
     * Fetch column.
     * 
     * @param columnName
     *            the column name
     * @param connection
     *            the connection
     * @param results
     *            the results
     * @param resultKeys
     *            the result keys
     * @return the list
     */
    private List fetchColumn(String columnName, Object connection, List results, Set<String> resultKeys)
    {
        for (String hashKey : resultKeys)
        {
            List columnValues = null;
            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).hmget(hashKey, columnName);
                // ((Transaction) connection).exec();
                ((RedisTransaction) resource).onExecute(((Transaction) connection));

                columnValues = (List) response.get();
            }
            else
            {
                columnValues = ((Jedis) connection).hmget(hashKey, columnName);
            }

            if (columnValues != null && !columnValues.isEmpty())
            {
                results.addAll(columnValues); // Currently returning list of
                                              // string as known issue
                                              // with
                                              // joint table concept!
            }

        }

        return results;
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
        Object connection = null;

        try
        {
            connection = getConnection();
            String valueAsStr = PropertyAccessorHelper.getString(columnValue);

            Set<String> results = null;

            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).zrangeByScore(getHashKey(tableName, valueAsStr),
                        getDouble(valueAsStr), getDouble(valueAsStr));
                // ((Transaction) connection).exec();
                ((RedisTransaction) resource).onExecute(((Transaction) connection));

                results = (Set<String>) response.get();
            }
            else
            {
                results = ((Jedis) connection).zrangeByScore(getHashKey(tableName, valueAsStr), getDouble(valueAsStr),
                        getDouble(valueAsStr));

            }

            List returnResults = new ArrayList();
            returnResults = fetchColumn(pKeyName, connection, returnResults, results);
            if (returnResults != null)
            {

                return returnResults.toArray(new Object[0]);
            }

        }
        finally
        {
            onCleanup(connection);
        }

        return null;
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
        Object connection = null;
        Pipeline pipeLine = null;
        try
        {

            connection = getConnection();

            if (isBoundTransaction())
            {
                pipeLine = ((Jedis) connection).pipelined();
            }

            String valueAsStr = PropertyAccessorHelper.getString(columnValue);
            Double score = getDouble(valueAsStr);
            Set<String> results = null;
            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).zrangeByScore(getHashKey(tableName, valueAsStr), score,
                        score);
                // ((Transaction) connection).exec();
                ((RedisTransaction) resource).onExecute(((Transaction) connection));

                results = (Set<String>) response.get();
            }
            else
            {
                results = ((Jedis) connection).zrangeByScore(getHashKey(tableName, valueAsStr), score, score);
            }
            // Set<String> results =
            // connection.zrangeByScore(getHashKey(tableName, valueAsStr),
            // score, score);

            if (results != null)
            {
                for (String rowKey : results)
                {
                    // byte[] hashKey = getEncodedBytes(getHashKey(tableName,
                    // rowKey));

                    Map<byte[], byte[]> columns = null;
                    columns = getColumns(connection, rowKey, columns);

                    for (byte[] column : columns.keySet()) // delete each
                                                           // column(e.g.
                    // field)
                    {
                        // connection.get(key)
                        String colName = PropertyAccessorFactory.STRING.fromBytes(String.class, columns.get(column));

                        if (resource != null && resource.isActive())
                        {
                            ((Transaction) connection).hdel(getEncodedBytes(rowKey), column); // delete
                            // record
                            ((Transaction) connection).zrem(getHashKey(tableName, colName), rowKey); // delete
                            // inverted
                            // index.

                        }
                        else
                        {
                            ((Jedis) connection).hdel(getEncodedBytes(rowKey), column); // delete
                            // record
                            ((Jedis) connection).zrem(getHashKey(tableName, colName), rowKey); // delete
                            // inverted
                            // index.

                        }
                    }
                }

            }
        }
        finally
        {
            if (pipeLine != null)
            {
                pipeLine.sync();
            }
            onCleanup(connection);
        }
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

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        Object[] ids = findIdsByColumn(entityMetadata.getTableName(), colName, colValue);
        List<Object> resultSet = new ArrayList<Object>();
        if (ids != null)
        {
            // just to insure uniqueness.

            for (Object id : new HashSet(Arrays.asList(ids)))
            {
                resultSet.add(find(entityClazz, id));
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
    public Class<RedisQuery> getQueryImplementor()
    {
        return RedisQuery.class;
    }

    /**
     * To supply configurations for jedis connection.
     * 
     * @param configurations
     *            the configurations
     */
    public void setConfig(Map<String, Object> configurations)
    {
        this.settings = configurations;
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
        Object connection = getConnection();
        // Create a hashset and populate data into it
        Pipeline pipeLine = null;
        if (isBoundTransaction())
        {
            pipeLine = ((Jedis) connection).pipelined();
        }
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
                        onDelete(node.getData(), node.getEntityId(), pipeLine != null ? pipeLine : connection);
                    }
                    else
                    {

                        List<RelationHolder> relationHolders = getRelationHolders(node);
                        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                node.getDataClass());

                        onPersist(metadata, node.getData(), node.getEntityId(), relationHolders,
                                pipeLine != null ? pipeLine : connection);
                    }
                    node.handlePostEvent();
                }
            }
        }
        finally
        {
            //
            if (pipeLine != null)
            {
                pipeLine.sync(); // send I/O.. as persist call. so no need to
                                 // read
                // response?
            }
            onCleanup(connection);
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
     * Find ids by column.
     * 
     * @param tableName
     *            the table name
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     * @return the object[]
     */
    private Object[] findIdsByColumn(String tableName, String columnName, Object columnValue)
    {
        Object connection = null;

        try
        {
            connection = getConnection();
            String valueAsStr = PropertyAccessorHelper.getString(columnValue);

            Set<String> results = null;

            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).zrangeByScore(getHashKey(tableName, columnName),
                        getDouble(valueAsStr), getDouble(valueAsStr));
                // ((Transaction) connection).exec();
                ((RedisTransaction) resource).onExecute(((Transaction) connection));

                results = (Set<String>) response.get();
            }
            else
            {
                results = ((Jedis) connection).zrangeByScore(getHashKey(tableName, columnName), getDouble(valueAsStr),
                        getDouble(valueAsStr));

            }
            if (results != null)
            {
                return results.toArray(new Object[0]);
            }

        }
        finally
        {
            onCleanup(connection);
        }

        return null;
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
     * Attribute wrapper.
     * 
     * @author vivek.mishra
     * 
     */
    class AttributeWrapper
    {

        /** The columns. */
        private Map<byte[], byte[]> columns;

        /** The indexes. */
        private Map<String, Double> indexes;

        /**
         * Instantiates a new attribute wrapper.
         */
        private AttributeWrapper()
        {
            columns = new HashMap<byte[], byte[]>();

            indexes = new HashMap<String, Double>();
        }

        /**
         * Instantiates a new attribute wrapper.
         * 
         * @param size
         *            the size
         */
        AttributeWrapper(int size)
        {
            columns = new HashMap<byte[], byte[]>(size);

            indexes = new HashMap<String, Double>(size);
        }

        /**
         * Adds the column.
         * 
         * @param key
         *            the key
         * @param value
         *            the value
         */
        private void addColumn(byte[] key, byte[] value)
        {
            columns.put(key, value);
        }

        /**
         * Adds the index.
         * 
         * @param key
         *            the key
         * @param score
         *            the score
         */
        private void addIndex(String key, Double score)
        {
            indexes.put(key, score);
        }

        /**
         * Gets the columns.
         * 
         * @return the columns
         */
        Map<byte[], byte[]> getColumns()
        {
            return columns;
        }

        /**
         * Gets the indexes.
         * 
         * @return the indexes
         */
        Map getIndexes()
        {
            return indexes;
        }

    }

    /**
     * Returns hash key.
     * 
     * @param tableName
     *            table name
     * @param rowKey
     *            row key
     * @return concatenated hash key
     */
    private String getHashKey(final String tableName, final String rowKey)
    {
        StringBuilder builder = new StringBuilder(tableName);
        builder.append(":");
        builder.append(rowKey);
        return builder.toString();
    }

    /**
     * Returns encoded bytes.
     * 
     * @param name
     *            field name.
     * @return encoded byte array.
     */
    byte[] getEncodedBytes(final String name)
    {
        try
        {
            if (name != null)
            {
                return name.getBytes(Constants.CHARSET_UTF8);
            }
        }
        catch (UnsupportedEncodingException e)
        {
            logger.error("Error during persist, Caused by:", e);
            throw new PersistenceException(e);
        }

        return null;
    }

    /**
     * Add inverted index in sorted set.
     * 
     * @param connection
     *            redis connection instance
     * @param wrapper
     *            attribute wrapper.
     * @param rowKey
     *            row key to be stor
     * @param metadata
     *            the metadata
     */
    private void addIndex(final Object connection, final AttributeWrapper wrapper, final String rowKey,
            final EntityMetadata metadata)
    {

        Indexer indexer = indexManager.getIndexer();

        if (indexer != null && indexer.getClass().getSimpleName().equals("RedisIndexer"))
        {
            // Add row key to list(Required for wild search over table).
            wrapper.addIndex(
                    getHashKey(metadata.getTableName(),
                            ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()), getDouble(rowKey));

            // Add row-key as inverted index as well needed for multiple clause
            // search with key and non row key.
            wrapper.addIndex(
                    getHashKey(metadata.getTableName(),
                            getHashKey(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), rowKey)),
                    getDouble(rowKey));

            indexer.index(metadata.getEntityClazz(), metadata, wrapper.getIndexes(), rowKey, null);
        }
    }

    /**
     * Deletes inverted indexes from redis.
     * 
     * @param connection
     *            redis instance.
     * @param wrapper
     *            attribute wrapper
     * @param member
     *            sorted set member name.
     */

    private void unIndex(final Object connection, final AttributeWrapper wrapper, final String member)
    {
        Set<String> keys = wrapper.getIndexes().keySet();
        for (String key : keys)
        {
            if (resource != null && resource.isActive())
            {
                ((Transaction) connection).zrem(key, member);

            }
            else
            {
                ((Pipeline) connection).zrem(key, member);

            }
        }
    }

    /**
     * On release connection.
     * 
     * @param connection
     *            redis connection instance.
     */
    private void onCleanup(Object connection)
    {
        // if not running within transaction boundary
        if (this.connection != null)
        {
            if (settings != null)
            {
                ((Jedis) connection).configResetStat();
            }
            factory.releaseConnection((Jedis) this.connection);
        }

        this.connection = null;
    }

    /*    *//**
     * Prepares composite key as a redis key.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @return redis key
     */
    /*
     * private String prepareCompositeKey(final EntityMetadata m, final
     * MetamodelImpl metaModel, final Object compositeKey) { // EmbeddableType
     * keyObject = //
     * metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
     * 
     * Field[] fields =
     * m.getIdAttribute().getBindableJavaType().getDeclaredFields();
     * 
     * StringBuilder stringBuilder = new StringBuilder(); for (Field f : fields)
     * { if (!ReflectUtils.isTransientOrStatic(f)) { // Attribute
     * compositeColumn = // keyObject.getAttribute(f.getName()); try { String
     * fieldValue = PropertyAccessorHelper.getString(compositeKey, f); // field
     * // value stringBuilder.append(fieldValue);
     * stringBuilder.append(COMPOSITE_KEY_SEPERATOR); } catch
     * (IllegalArgumentException e) {
     * logger.error("Error during persist, Caused by:", e); throw new
     * PersistenceException(e); } } }
     * 
     * if (stringBuilder.length() > 0) {
     * stringBuilder.deleteCharAt(stringBuilder
     * .lastIndexOf(COMPOSITE_KEY_SEPERATOR)); } return
     * stringBuilder.toString(); }
     */
    /**
     * Wraps entity attributes into byte[] and return instance of attribute
     * wrapper.
     * 
     * @param entityMetadata
     * @param entity
     * @return
     */
    private AttributeWrapper wrap(EntityMetadata entityMetadata, Object entity)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();

        // attributes can be null??? i guess NO
        AttributeWrapper wrapper = new AttributeWrapper(attributes.size());

        List<String> relationNames = entityMetadata.getRelationNames();

        // PropertyAccessorHelper.get(entity,
        for (Attribute attr : attributes)
        {
            if (/* !entityMetadata.getIdAttribute().equals(attr) && */!attr.isAssociation())
            {
                if (metaModel.isEmbeddable(((AbstractAttribute) attr).getBindableJavaType()))
                {
                    EmbeddableType embeddableAttribute = metaModel.embeddable(((AbstractAttribute) attr)
                            .getBindableJavaType());

                    Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) attr.getJavaMember());

                    Set<Attribute> embeddedAttributes = embeddableAttribute.getAttributes();

                    for (Attribute attrib : embeddedAttributes)
                    {
                        addToWrapper(entityMetadata, wrapper, embeddedObject, attrib, attr);
                    }

                }
                else
                {
                    addToWrapper(entityMetadata, wrapper, entity, attr);
                }
            }
            else if (attributes.size() == 1) // means it is only a key! weird
                                             // but possible negative
                                             // scenario
            {
                byte[] value = PropertyAccessorHelper.get(entity, (Field) attr.getJavaMember());
                byte[] name;
                name = getEncodedBytes(((AbstractAttribute) attr).getJPAColumnName());

                // add column name as key and value as value
                wrapper.addColumn(name, value);

            }
        }

        return wrapper;
    }

    /**
     * Adds field to wrapper.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param wrapper
     *            the wrapper
     * @param resultedObject
     *            the resulted object
     * @param attrib
     *            the attrib
     */
    private void addToWrapper(EntityMetadata entityMetadata, AttributeWrapper wrapper, Object resultedObject,
            Attribute attrib)
    {
        addToWrapper(entityMetadata, wrapper, resultedObject, attrib, null);
    }

    /**
     * Wraps entity attributes into redis format byte[].
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param wrapper
     *            the wrapper
     * @param embeddedObject
     *            the embedded object
     * @param attrib
     *            the attrib
     * @param embeddedAttrib
     *            the embedded attrib
     */
    private void addToWrapper(EntityMetadata entityMetadata, AttributeWrapper wrapper, Object embeddedObject,
            Attribute attrib, Attribute embeddedAttrib)
    {
        if (embeddedObject == null)
        {
            return;
        }
        
        byte[] value = PropertyAccessorHelper.get(embeddedObject, (Field) attrib.getJavaMember());
        byte[] name;
        if (value != null)
        {
            if (embeddedAttrib == null)
            {
                name = getEncodedBytes(((AbstractAttribute) attrib).getJPAColumnName());
            }
            else
            {
                name = getEncodedBytes(getHashKey(embeddedAttrib.getName(),
                        ((AbstractAttribute) attrib).getJPAColumnName()));
            }
            // add column name as key and value as value
            wrapper.addColumn(name, value);
            // // {tablename:columnname,hashcode} for value

            // selective indexing.
            if (entityMetadata.getIndexProperties().containsKey(((AbstractAttribute) attrib).getJPAColumnName()))
            {
                String valueAsStr = PropertyAccessorHelper.getString(embeddedObject, (Field) attrib.getJavaMember());
                wrapper.addIndex(
                        getHashKey(entityMetadata.getTableName(), ((AbstractAttribute) attrib).getJPAColumnName()),
                        getDouble(valueAsStr));

                wrapper.addIndex(
                        getHashKey(entityMetadata.getTableName(),
                                getHashKey(((AbstractAttribute) attrib).getJPAColumnName(), valueAsStr)),
                        getDouble(valueAsStr));
            }
        }
    }

    /**
     * Unwraps redis results into entity.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param results
     *            the results
     * @param key
     *            the key
     * @return the object
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private Object unwrap(EntityMetadata entityMetadata, Map<byte[], byte[]> results, Object key)
            throws InstantiationException, IllegalAccessException
    {

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        List<String> relationNames = entityMetadata.getRelationNames();
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        Map<String, Object> relations = new HashMap<String, Object>();
        Object entity = null;

        // Set<Attribute> attributes = entityType.getAttributes();

        Set<byte[]> columnNames = results.keySet();
        for (byte[] nameInByte : columnNames)
        {
            if (entity == null)
            {
                entity = KunderaCoreUtils.createNewInstance(entityMetadata.getEntityClazz());
            }

            String columnName = PropertyAccessorFactory.STRING.fromBytes(String.class, nameInByte);

            byte[] value = results.get(nameInByte);
            String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();

            if (columnName != null && !columnName.equals(discriminatorColumn))
            {
                String fieldName = entityMetadata.getFieldName(columnName);

                if (fieldName != null)
                {
                    Attribute attribute = entityType.getAttribute(fieldName);

                    if (relationNames != null && relationNames.contains(columnName))
                    {
                        Field field = (Field) attribute.getJavaMember();
                        EntityMetadata associationMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                ((AbstractAttribute) attribute).getBindableJavaType());
                        relations.put(columnName, PropertyAccessorHelper.getObject(associationMetadata.getIdAttribute()
                                .getBindableJavaType(), value));
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), value);
                    }
                }
                else
                {
                    // means it might be an embeddable field, if not simply omit
                    // this field.

                    if (StringUtils.contains(columnName, ":"))
                    {
                        StringTokenizer tokenizer = new StringTokenizer(columnName, ":");
                        while (tokenizer.hasMoreTokens())
                        {
                            String embeddedFieldName = tokenizer.nextToken();
                            String embeddedColumnName = tokenizer.nextToken();

                            Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(entityMetadata
                                    .getEntityClazz());

                            EmbeddableType embeddableAttribute = embeddables.get(embeddedFieldName);
                            
                            AbstractAttribute attrib = null;
                            Iterator itr = embeddableAttribute.getAttributes().iterator();
                            while (itr.hasNext())
                            {
                                attrib = (AbstractAttribute) itr.next();
                                if (attrib.getJPAColumnName().equals(embeddedColumnName))
                                {
                                    break;
                                }
                            }

                            Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) entityType
                                    .getAttribute(embeddedFieldName).getJavaMember());

                            if (embeddedObject == null)
                            {
                                embeddedObject = KunderaCoreUtils.createNewInstance(((AbstractAttribute) entityType
                                        .getAttribute(embeddedFieldName)).getBindableJavaType());

                                PropertyAccessorHelper.set(entity, (Field) entityType.getAttribute(embeddedFieldName)
                                        .getJavaMember(), embeddedObject);
                            }

                            PropertyAccessorHelper.set(embeddedObject, (Field) attrib.getJavaMember(), value);
                            // PropertyAccessorHelper.

                        }
                    }
                    // It might be a case of embeddable attribute.

                }
            }

        }

        if (entity != null)
        {
            Class javaType = entityMetadata.getIdAttribute().getBindableJavaType();

            if (!metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType())
                    && key.getClass().isAssignableFrom(String.class) && !key.getClass().equals(javaType))
            {
                key = PropertyAccessorFactory.getPropertyAccessor(javaType).fromString(javaType, key.toString());
            }
//            PropertyAccessorHelper.set(entity, (Field) entityMetadata.getIdAttribute().getJavaMember(), key);
        }
        if (!relations.isEmpty())
        {
            return new EnhanceEntity(entity, key, relations);
        }

        return entity;
    }

    /**
     * On execute query.
     * 
     * @param queryParameter
     *            the query parameter
     * @param entityClazz
     *            the entity clazz
     * @return the list
     */
    List onExecuteQuery(RedisQueryInterpreter queryParameter, Class entityClazz)
    {
        /**
         * Find a list of id's and then call findById for each!
         */
        Object connection = null;
        List<Object> results = new ArrayList<Object>();
        try
        {
            connection = getConnection();
            Set<String> rowKeys = new HashSet<String>();
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
            String printQuery = null;

            if (showQuery)
            {
                printQuery = "Fetching primary key from " + entityMetadata.getTableName() + " corresponding to ";
            }

            if (queryParameter.getClause() != null && !queryParameter.isByRange())
            {
                String destStore = entityClazz.getSimpleName() + System.currentTimeMillis();

                Map<String, Object> fieldSets = queryParameter.getFields();

                Set<String> keySets = new HashSet<String>(fieldSets.size());
                // byte[][] keys = new byte[][fieldSets.size()];
                for (String column : fieldSets.keySet())
                {
                    String valueAsStr = PropertyAccessorHelper.getString(fieldSets.get(column));
                    String key = getHashKey(entityMetadata.getTableName(), getHashKey(column, valueAsStr));
                    keySets.add(key);
                    if (showQuery)
                    {
                        printQuery = printQuery + key + " and ";
                    }
                }

                if (showQuery)
                {
                    printQuery = printQuery.substring(0, printQuery.lastIndexOf(" and "));
                }

                if (queryParameter.getClause().equals(Clause.INTERSECT))
                {
                    KunderaCoreUtils.printQuery(printQuery, showQuery);

                    if (resource != null && resource.isActive())
                    {
                        ((Transaction) connection).zinterstore(destStore, keySets.toArray(new String[] {}));
                    }
                    else
                    {
                        ((Jedis) connection).zinterstore(destStore, keySets.toArray(new String[] {}));
                    }
                }
                else
                {
                    if (showQuery)
                    {
                        KunderaCoreUtils.printQuery(printQuery.replaceAll("and", "or"), showQuery);
                    }

                    if (resource != null && resource.isActive())
                    {
                        ((Transaction) connection).zunionstore(destStore, keySets.toArray(new String[] {}));
                    }
                    else
                    {
                        ((Jedis) connection).zunionstore(destStore, keySets.toArray(new String[] {}));
                    }
                }

                if (resource != null && resource.isActive())
                {
                    Response response = ((Transaction) connection).zrange(destStore, 0, -1);
                    // ((Transaction) connection).exec();
                    ((RedisTransaction) resource).onExecute(((Transaction) connection));

                    rowKeys = (Set<String>) response.get();
                    // connection = reInitialize(connection, rowKeys);
                    //
                    // ((Transaction) connection).del(destStore);

                }
                else
                {
                    rowKeys = ((Jedis) connection).zrange(destStore, 0, -1);
                    ((Jedis) connection).del(destStore);
                }

                // delete intermediate store after find.
                //
                // means it is a query over sorted set.
            }
            else if (queryParameter.isByRange())
            {
                // means query over a single sorted set with range
                Map<String, Double> minimum = queryParameter.getMin();
                Map<String, Double> maximum = queryParameter.getMax();

                String column = minimum.keySet().iterator().next();
                KunderaCoreUtils.printQuery(printQuery + column + " between " + minimum + " and " + maximum, showQuery);
                if (resource != null && resource.isActive())
                {
                    Response response = ((Transaction) connection)
                            .zrangeByScore(getHashKey(entityMetadata.getTableName(), column), minimum.get(column),
                                    maximum.get(column));
                    // ((Transaction) connection).exec();
                    ((RedisTransaction) resource).onExecute(((Transaction) connection));

                    rowKeys = (Set<String>) response.get();
                    // connection = reInitialize(connection, rowKeys);
                }
                else
                {
                    rowKeys = ((Jedis) connection).zrangeByScore(getHashKey(entityMetadata.getTableName(), column),
                            minimum.get(column), maximum.get(column));
                }
            }
            else if (queryParameter.isById())
            {
                Map<String, Object> fieldSets = queryParameter.getFields();

                results = findAllColumns(entityClazz, (queryParameter.getColumns() != null ? queryParameter
                        .getColumns().toArray(new byte[][] {}) : null), fieldSets.values().toArray());
                return results;
            }
            else if (queryParameter.getFields() != null)
            {
                Set<String> columns = queryParameter.getFields().keySet();

                for (String column : columns)
                {
                    // ideally it will always be 1 value in map, else it will go
                    // it queryParameter.getClause() will not be null!
                    Double value = getDouble(PropertyAccessorHelper.getString(queryParameter.getFields().get(column)));
                    if (resource != null && resource.isActive())
                    {
                        Response response = ((Transaction) connection).zrangeByScore(
                                getHashKey(entityMetadata.getTableName(), column), value, value);
                        // ((Transaction) connection).exec();
                        ((RedisTransaction) resource).onExecute(((Transaction) connection));

                        rowKeys = (Set<String>) response.get();
                        // connection = reInitialize(connection, rowKeys);

                    }
                    else
                    {
                        rowKeys = ((Jedis) connection).zrangeByScore(getHashKey(entityMetadata.getTableName(), column),
                                value, value);
                    }
                }
            }
            else
            {
                if (resource != null && resource.isActive())
                {
                    Response response = ((Transaction) connection).zrange(
                            getHashKey(entityMetadata.getTableName(),
                                    ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), 0, -1);
                    // resource.onCommit()
                    // ((Transaction) connection).exec();
                    ((RedisTransaction) resource).onExecute(((Transaction) connection));

                    rowKeys = new HashSet<String>((Collection<? extends String>) response.get());
                }
                else
                {
                    rowKeys = new HashSet<String>(((Jedis) connection).zrange(
                            getHashKey(entityMetadata.getTableName(),
                                    ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), 0, -1));
                }
            }

            for (String k : rowKeys)
            {
                connection = reInitialize(connection, rowKeys);
                Object record = fetch(entityClazz, k, connection, (queryParameter.getColumns() != null ? queryParameter
                        .getColumns().toArray(new byte[][] {}) : null));
                if (record != null)
                {
                    results.add(record);
                }
            }

        }
        catch (InstantiationException e)
        {
            logger.error("Error during persist, Caused by:", e);
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            logger.error("Error during persist, Caused by:", e);
            throw new PersistenceException(e);
        }
        catch (Exception e)
        {
            logger.error("Error during persist, Caused by:", e);
            throw new PersistenceException(e);
        }
        finally
        {
            onCleanup(connection);

        }

        return results;
    }

    /**
     * Re initialize.
     * 
     * @param connection
     *            the connection
     * @param rowKeys
     *            the row keys
     * @return the object
     */
    private Object reInitialize(Object connection, Set<String> rowKeys)
    {
        /*
         * if(!rowKeys.isEmpty()) {
         */
        connection = getConnection();
        // }
        return connection;
    }

    /**
     * Find all columns.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param columns
     *            the columns
     * @param keys
     *            the keys
     * @return the list
     */
    private <E> List<E> findAllColumns(Class<E> entityClass, byte[][] columns, Object... keys)
    {
        Object connection = getConnection();
        // connection.co
        List results = new ArrayList();
        try
        {
            for (Object key : keys)
            {
                Object result = fetch(entityClass, key, connection, columns);
                if (result != null)
                {
                    results.add(result);
                }
            }
        }
        catch (InstantiationException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            logger.error("Error during find by key:", e);
            throw new PersistenceException(e);
        }
        return results;
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
        setConfig(properties);
        for (String key : properties.keySet())
        {
            Object value = properties.get(key);
            if (key.equals(PersistenceProperties.KUNDERA_BATCH_SIZE) && value instanceof Integer)
            {
                Integer batchSize = (Integer) value;
                ((RedisClient) client).setBatchSize(batchSize);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.TransactionBinder#bind(com.impetus.kundera
     * .persistence.TransactionResource)
     */
    @Override
    public void bind(TransactionResource resource)
    {
        // Not checking for type of TransactionRes
        if (resource != null && resource instanceof RedisTransaction)
        {
            this.resource = resource;
        }
        else
        {
            throw new KunderaTransactionException("Invalid transaction resource provided:" + resource
                    + " Should have been an instance of :" + RedisTransaction.class);
        }
    }

    /**
     * Returns jedis connection.
     * 
     * @return jedis resource.
     */
    private Object getConnection()
    {
        /*
         * Jedis connection = factory.getConnection();
         * 
         * // If resource is not null means a transaction in progress.
         * 
         * if (settings != null) { for (String key : settings.keySet()) {
         * connection.configSet(key, settings.get(key).toString()); } }
         * 
         * if (resource != null && resource.isActive()) { return
         * ((RedisTransaction) resource).bindResource(connection); } else {
         * return connection; } if (resource == null || (resource != null &&
         * !resource.isActive()))
         */
        // means either transaction resource is not bound or it is not active,
        // but connection has already by initialized
        if (isBoundTransaction() && this.connection != null)
        {
            return this.connection;
        }

        // if running within transaction boundary.
        if (resource != null && resource.isActive())
        {
            // no need to get a connection from pool, as nested MULTI is not yet
            // supported.
            if (((RedisTransaction) resource).isResourceBound())
            {
                return ((RedisTransaction) resource).getResource();
            }
            else
            {
                Jedis conn = getAndSetConnection();
                return ((RedisTransaction) resource).bindResource(conn);
            }

        }
        else
        {
            Jedis conn = getAndSetConnection();
            return conn;
        }
    }

    /**
     * Gets the and set connection.
     * 
     * @return the and set connection
     */
    private Jedis getAndSetConnection()
    {
        Jedis conn = factory.getConnection();
        this.connection = conn;
        // If resource is not null means a transaction in progress.

        if (settings != null)
        {
            for (String key : settings.keySet())
            {
                conn.configSet(key, settings.get(key).toString());
            }
        }
        return conn;
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
        String batch_Size = null;
        if (puProperties != null)
        {
            batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                    : null;
            if (batch_Size != null)
            {
                setBatchSize(Integer.valueOf(batch_Size));
            }
        }
        else if (batch_Size == null)
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
    private void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param rlHolders
     *            the rl holders
     * @param connection
     *            the connection
     */
    private void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders,
            Object connection)
    {
        // first open a pipeline
        AttributeWrapper wrapper = wrap(entityMetadata, entity);

        // add relations.

        if (rlHolders != null)
        {
            for (RelationHolder relation : rlHolders)
            {
                String name = relation.getRelationName();
                Object value = relation.getRelationValue();
                byte[] valueInBytes = PropertyAccessorHelper.getBytes(value);
                byte[] nameInBytes = getEncodedBytes(name);
                String valueAsStr = PropertyAccessorHelper.getString(value);
                wrapper.addColumn(nameInBytes, valueInBytes);
                wrapper.addIndex(getHashKey(entityMetadata.getTableName(), name), getDouble(valueAsStr));

                // this index is required to work for UNION/INTERSECT
                // support.

                wrapper.addIndex(getHashKey(entityMetadata.getTableName(), getHashKey(name, valueAsStr)),
                        getDouble(valueAsStr));
            }
        }

        // prepareCompositeKey

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            byte[] valueInBytes = PropertyAccessorHelper.getBytes(discrValue);
            byte[] nameInBytes = getEncodedBytes(discrColumn);
            wrapper.addColumn(nameInBytes, valueInBytes);
            wrapper.addIndex(getHashKey(entityMetadata.getTableName(), discrColumn), getDouble(discrValue));
            wrapper.addIndex(getHashKey(entityMetadata.getTableName(), getHashKey(discrColumn, discrValue)),
                    getDouble(discrValue));
        }

        String rowKey = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, id);
        }
        else
        {
            ObjectAccessor accessor = new ObjectAccessor();
            rowKey = accessor.toString(id);
            // rowKey = /*PropertyAccessorHelper.getString(entity, (Field)
            // entityMetadata.getIdAttribute().getJavaMember())*/ ;
        }

        String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);

        if (resource != null && resource.isActive())
        {
            ((Transaction) connection).hmset(getEncodedBytes(hashKey), wrapper.getColumns());
        }
        else
        {
            ((Pipeline) connection).hmset(getEncodedBytes(hashKey), wrapper.getColumns());
        }

        // Add inverted indexes for column based search.

        // // Add row key to list(Required for wild search over table).
        //
        // wrapper.addIndex(getHashKey(entityMetadata.getTableName(),
        // ((AbstractAttribute)
        // entityMetadata.getIdAttribute()).getJPAColumnName()),
        // getDouble(rowKey));
        //
        // // Add row-key as inverted index as well needed for multiple clause
        // // search with key and non row key.
        // wrapper.addIndex(getHashKey(
        // entityMetadata.getTableName(),
        // getHashKey(((AbstractAttribute)
        // entityMetadata.getIdAttribute()).getJPAColumnName(), rowKey)),
        // getDouble(rowKey));
        KunderaCoreUtils.printQuery(
                "Persist data into " + entityMetadata.getSchema() + "." + entityMetadata.getTableName()
                        + " for primary key " + rowKey, showQuery);
        addIndex(connection, wrapper, rowKey, entityMetadata);

    }

    /**
     * On delete.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     * @param connection
     *            the connection
     */
    private void onDelete(Object entity, Object pKey, Object connection)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        AttributeWrapper wrapper = wrap(entityMetadata, entity);

        Set<byte[]> columnNames = wrapper.columns.keySet();

        String rowKey = null;

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, pKey);
        }
        else
        {
            rowKey = PropertyAccessorHelper.getString(entity, (Field) entityMetadata.getIdAttribute().getJavaMember());
        }

        for (byte[] name : columnNames)
        {
            if (resource != null && resource.isActive())
            {
                ((Transaction) connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey),
                        PropertyAccessorFactory.STRING.fromBytes(String.class, name));

            }
            else
            {
                ((Pipeline) connection).hdel(getHashKey(entityMetadata.getTableName(), rowKey),
                        PropertyAccessorFactory.STRING.fromBytes(String.class, name));

            }
        }

        // Delete relation values.

        deleteRelation(connection, entityMetadata, rowKey);

        // Delete inverted indexes.
        unIndex(connection, wrapper, rowKey);

        if (resource != null && resource.isActive())
        {
            ((Transaction) connection).zrem(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), rowKey);

        }
        else
        {
            ((Pipeline) connection).zrem(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), rowKey);

        }
        KunderaCoreUtils.printQuery("Delete data from:" + entityMetadata.getTableName() + "for primary key: " + rowKey,
                showQuery);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#indexNode(com.impetus.kundera.graph
     * .Node, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    protected void indexNode(Node node, EntityMetadata entityMetadata)
    {
        // Do nothing as
        if (this.indexManager.getIndexer() != null
                && !this.indexManager.getIndexer().getClass().getSimpleName().equals("RedisIndexer"))
        {
            super.indexNode(node, entityMetadata);
        }
    }

    /**
     * Initialize indexer.
     */
    private void initializeIndexer()
    {
        if (this.indexManager.getIndexer() != null
                && this.indexManager.getIndexer().getClass().getSimpleName().equals("RedisIndexer"))
        {
            ((RedisIndexer) this.indexManager.getIndexer()).assignConnection(getConnection());
        }
    }

    /**
     * Checks if is bound transaction.
     * 
     * @return true, if is bound transaction
     */
    private boolean isBoundTransaction()
    {
        return resource == null || (resource != null && !resource.isActive());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(RedisIdGenerator.class);
    }

}