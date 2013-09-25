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
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
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
        TransactionBinder, SequenceGenerator
{
    /**
     * Reference to redis client factory.
     */
    private RedisClientFactory factory;

    private EntityReader reader;

    private Map<String, Object> settings;

    /** list of nodes for batch processing. */
    private List<Node> nodes = new ArrayList<Node>();

    private TransactionResource resource;

    /** batch size. */
    private int batchSize;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private static final String COMPOSITE_KEY_SEPERATOR = "\001";

    private Jedis connection;

    RedisClient(final RedisClientFactory factory, final String persistenceUnit)
    {
        this.factory = factory;
        reader = new RedisEntityReader();
        this.indexManager = factory.getIndexManager();
        this.persistenceUnit = persistenceUnit;
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
            if (resource == null)
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

    private double getDouble(String valueAsStr)
    {
        return StringUtils.isNumeric(valueAsStr) ? Double.parseDouble(valueAsStr) : Double
                .parseDouble(((Integer) valueAsStr.hashCode()).toString());
    }

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

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz);

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        String rowKey = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, metaModel, key);
        }
        else
        {
            ObjectAccessor accessor = new ObjectAccessor();

            rowKey = accessor.toString(key);/*
                                             * PropertyAccessorHelper.getString(key
                                             * );
                                             */
        }

        String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);

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
                    ((Transaction) connection).exec();

                    fieldValues = (List<byte[]>) response.get();

                }
                else
                {
                    fieldValues = ((Jedis) connection).hmget(getEncodedBytes(hashKey), fields);
                }

                if (fieldValues != null && !fieldValues.isEmpty())
                {
                    for (int i = 0; i < fields.length; i++)
                    {
                        columns.put(fields[i], fieldValues.get(i));
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

    private Map<byte[], byte[]> getColumns(Object connection, String hashKey, Map<byte[], byte[]> columns)
    {
        if (resource != null && resource.isActive())
        {
            // Why transaction API returns response in byte[] format/?
            Response response = ((Transaction) connection).hgetAll(getEncodedBytes(hashKey));
            ((Transaction) connection).exec();
            Map<String, String> cols = (Map<String, String>) response.get();

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

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        throw new UnsupportedOperationException("Method not supported!");
    }

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
        Object connection = getConnection();
        Pipeline pipeLine = null;
        try
        {
            if (resource == null)
            {
                pipeLine = ((Jedis) connection).pipelined();
                onDelete(entity, pKey, pipeLine);
            }
            else
            {
                onDelete(entity, pKey, connection);
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

    /**
     * On delete relation.
     * 
     * @param connection
     *            connection instance.
     * @param entityMetadata
     *            entity metadata.
     * @param rowKey
     *            row key.
     * @param relationHolders
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
            if (resource == null)
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
                ((Transaction) connection).exec();
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
            return results;
        }
        finally
        {
            onCleanup(connection);
        }
    }

    /**
     * @param columnName
     * @param connection
     * @param results
     * @param resultKeys
     */
    private List fetchColumn(String columnName, Object connection, List results, Set<String> resultKeys)
    {
        for (String hashKey : resultKeys)
        {
            List columnValues = null;
            if (resource != null && resource.isActive())
            {
                Response response = ((Transaction) connection).hmget(hashKey, columnName);
                ((Transaction) connection).exec();

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
                ((Transaction) connection).exec();

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

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        Object connection = null;
        Pipeline pipeLine = null;
        try
        {

            connection = getConnection();

            if (resource == null)
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
                ((Transaction) connection).exec();

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

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
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

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<RedisQuery> getQueryImplementor()
    {
        return RedisQuery.class;
    }

    /**
     * To supply configurations for jedis connection.
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
        if (resource == null)
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
                        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());

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
            nodes = null;
            nodes = new ArrayList<Node>();
        }
    }

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
                ((Transaction) connection).exec();

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
    private class AttributeWrapper
    {
        private Map<byte[], byte[]> columns;

        private Map<String, Double> indexes;

        private AttributeWrapper()
        {
            columns = new HashMap<byte[], byte[]>();

            indexes = new HashMap<String, Double>();
        }

        /**
         * @param columns
         * @param indexes
         */
        AttributeWrapper(int size)
        {
            columns = new HashMap<byte[], byte[]>(size);

            indexes = new HashMap<String, Double>(size);
        }

        private void addColumn(byte[] key, byte[] value)
        {
            columns.put(key, value);
        }

        private void addIndex(String key, Double score)
        {
            indexes.put(key, score);
        }

        Map<byte[], byte[]> getColumns()
        {
            return columns;
        }

        Map<String, Double> getIndexes()
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
    private byte[] getEncodedBytes(final String name)
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
     */
    private void addIndex(final Object connection, final AttributeWrapper wrapper, final String rowKey)
    {
        Set<String> indexKeys = wrapper.getIndexes().keySet();
        for (String idx_Name : indexKeys)
        {
            if (resource != null && resource.isActive())
            {
                ((Transaction) connection).zadd(idx_Name, wrapper.getIndexes().get(idx_Name), rowKey);

            }
            else
            {
                ((Pipeline) connection).zadd(idx_Name, wrapper.getIndexes().get(idx_Name), rowKey);

            }
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
        if (resource == null && connection != null)
        {
            if (settings != null)
            {
                ((Jedis) connection).configResetStat();
            }
            factory.releaseConnection((Jedis) connection);
        }
    }

    /*    *//**
     * Prepares composite key as a redis key.
     * 
     * @param m
     *            entity metadata
     * @param metaModel
     *            meta model.
     * @param compositeKey
     *            composite key instance
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

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();

        // attributes can be null??? i guess NO
        AttributeWrapper wrapper = new AttributeWrapper(attributes.size());

        List<String> relationNames = entityMetadata.getRelationNames();

        // PropertyAccessorHelper.get(entity,
        for (Attribute attr : attributes)
        {
            if (!entityMetadata.getIdAttribute().equals(attr) && !attr.isAssociation())
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
     * @param wrapper
     * @param resultedObject
     * @param attrib
     */
    private void addToWrapper(EntityMetadata entityMetadata, AttributeWrapper wrapper, Object resultedObject,
            Attribute attrib)
    {
        addToWrapper(entityMetadata, wrapper, resultedObject, attrib, null);
    }

    /**
     * Wraps entity attributes into redis format byte[]
     * 
     * @param entityMetadata
     * @param wrapper
     * @param embeddedObject
     * @param attrib
     * @param embeddedAttrib
     */
    private void addToWrapper(EntityMetadata entityMetadata, AttributeWrapper wrapper, Object embeddedObject,
            Attribute attrib, Attribute embeddedAttrib)
    {
        byte[] value = PropertyAccessorHelper.get(embeddedObject, (Field) attrib.getJavaMember());
        String valueAsStr = PropertyAccessorHelper.getString(embeddedObject, (Field) attrib.getJavaMember());
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
            wrapper.addIndex(
                    getHashKey(entityMetadata.getTableName(), ((AbstractAttribute) attrib).getJPAColumnName()),
                    getDouble(valueAsStr));

            wrapper.addIndex(
                    getHashKey(entityMetadata.getTableName(),
                            getHashKey(((AbstractAttribute) attrib).getJPAColumnName(), valueAsStr)),
                    getDouble(valueAsStr));
        }
    }

    /**
     * Unwraps redis results into entity.
     * 
     * @param entityMetadata
     * @param results
     * @param key
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private Object unwrap(EntityMetadata entityMetadata, Map<byte[], byte[]> results, Object key)
            throws InstantiationException, IllegalAccessException
    {

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
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
                entity = entityMetadata.getEntityClazz().newInstance();
            }

            String columnName = PropertyAccessorFactory.STRING.fromBytes(String.class, nameInByte);

            byte[] value = results.get(nameInByte);
            String fieldName = entityMetadata.getFieldName(columnName);

            if (fieldName != null)
            {
                Attribute attribute = entityType.getAttribute(fieldName);

                if (relationNames != null && relationNames.contains(columnName))
                {
                    Field field = (Field) attribute.getJavaMember();
                    EntityMetadata associationMetadata = KunderaMetadataManager
                            .getEntityMetadata(((AbstractAttribute) attribute).getBindableJavaType());
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

                        Attribute attrib = embeddableAttribute.getAttribute(embeddedColumnName);

                        Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) entityType
                                .getAttribute(embeddedFieldName).getJavaMember());

                        if (embeddedObject == null)
                        {
                            embeddedObject = ((AbstractAttribute) entityType.getAttribute(embeddedFieldName))
                                    .getBindableJavaType().newInstance();
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

        if (entity != null)
        {
            Class javaType = entityMetadata.getIdAttribute().getJavaType();

            if (!metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                key = PropertyAccessorFactory.getPropertyAccessor(javaType).fromString(javaType, key.toString());
            }
            PropertyAccessorHelper.set(entity, (Field) entityMetadata.getIdAttribute().getJavaMember(), key);
        }
        if (!relations.isEmpty())
        {
            return new EnhanceEntity(entity, key, relations);
        }

        return entity;
    }

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
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
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
                }

                if (queryParameter.getClause().equals(Clause.INTERSECT))
                {
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
                    ((Transaction) connection).exec();

                    rowKeys = (Set<String>) response.get();
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

                if (resource != null && resource.isActive())
                {
                    Response response = ((Transaction) connection)
                            .zrangeByScore(getHashKey(entityMetadata.getTableName(), column), minimum.get(column),
                                    maximum.get(column));
                    ((Transaction) connection).exec();

                    rowKeys = (Set<String>) response.get();

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

                results = findAllColumns(entityClazz, (byte[][]) (queryParameter.getColumns() != null ? queryParameter
                        .getColumns().toArray() : null), fieldSets.values().toArray());
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
                        ((Transaction) connection).exec();

                        rowKeys = (Set<String>) response.get();

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
                    ((Transaction) connection).exec();

                    rowKeys = new HashSet<String>((Collection<? extends String>) response.get());
                }
                else
                {
                    rowKeys = new HashSet<String>(((Jedis) connection).zrange(
                            getHashKey(entityMetadata.getTableName(),
                                    ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), 0, -1));
                }
            }

            // fetch fr
            for (String k : rowKeys)
            {

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
        finally
        {
            onCleanup(connection);

        }

        return results;
    }

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
         * return connection; }
         */
        if (resource == null && this.connection != null)
        {
            return this.connection;
        }

        Jedis conn = factory.getConnection();

        // If resource is not null means a transaction in progress.

        if (settings != null)
        {
            for (String key : settings.keySet())
            {
                conn.configSet(key, settings.get(key).toString());
            }
        }

        if (resource != null && resource.isActive())
        {
            return ((RedisTransaction) resource).bindResource(conn);
        }
        else
        {
            this.connection = conn;
            return conn;
        }
    }

    /**
     * @param persistenceUnit
     * @param puProperties
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
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    private void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }

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

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        String rowKey = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, metaModel, id);
        }
        else
        {
            ObjectAccessor accessor = new ObjectAccessor();
            rowKey = accessor.toString(id);
            // rowKey = /*PropertyAccessorHelper.getString(entity, (Field)
            // entityMetadata.getIdAttribute().getJavaMember())*/ ;
        }

        String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);

        // Add row key to list(Required for wild search over table).

        if (resource != null && resource.isActive())
        {
            ((Transaction) connection).hmset(getEncodedBytes(hashKey), wrapper.getColumns());

            ((Transaction) connection).zadd(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()),
                    getDouble(rowKey), rowKey);

            // Add row-key as inverted index as well needed for multiple clause
            // search with key and non row key.

            ((Transaction) connection)
                    .zadd(getHashKey(
                            entityMetadata.getTableName(),
                            getHashKey(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), rowKey)),
                            getDouble(rowKey), rowKey);
        }
        else
        {
            ((Pipeline) connection).hmset(getEncodedBytes(hashKey), wrapper.getColumns());

            ((Pipeline) connection).zadd(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()),
                    getDouble(rowKey), rowKey);

            // Add row-key as inverted index as well needed for multiple clause
            // search with key and non row key.

            ((Pipeline) connection)
                    .zadd(getHashKey(
                            entityMetadata.getTableName(),
                            getHashKey(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), rowKey)),
                            getDouble(rowKey), rowKey);
        }

        // Add inverted indexes for column based search.
        addIndex(connection, wrapper, rowKey);

    }

    private void onDelete(Object entity, Object pKey, Object connection)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        AttributeWrapper wrapper = wrap(entityMetadata, entity);

        Set<byte[]> columnNames = wrapper.columns.keySet();

        String rowKey = null;

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(entityMetadata, metaModel, pKey);
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
    }

    @Override
    public Object generate(SequenceGeneratorDiscriptor discriptor)
    {
        Jedis jedis = factory.getConnection();

        Long latestCount = jedis.incr(getEncodedBytes(discriptor.getSequenceName()));
        if (latestCount == 1)
        {
            return discriptor.getInitialValue();
        }
        else
        {
            return (latestCount - 1) * discriptor.getAllocationSize();
        }
    }
}