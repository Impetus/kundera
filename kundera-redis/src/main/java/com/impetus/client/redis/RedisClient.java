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
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.impetus.client.redis.RedisQueryInterpreter.Clause;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Redis client implementation for REDIS.
 * 
 * Pending: 
 * 1) External properties junit.
 * 2) Performance number work
 * 3) Enable kundera-tests for redis.
 * 
 * @author vivek.mishra
 */
public class RedisClient extends ClientBase implements Client<RedisQuery>, Batcher, ClientPropertiesSetter
{
    /**
     * Reference to redis client factory.
     */
    private RedisClientFactory factory;

    private EntityReader reader;

    private Map<String, Object> settings;

    
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private static final String COMPOSITE_KEY_SEPERATOR = "\001";

    RedisClient(final RedisClientFactory factory, final String persistenceUnit)
    {
        this.factory = factory;
        reader = new RedisEntityReader();
        this.indexManager = factory.getIndexManager();
        this.persistenceUnit = persistenceUnit;
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
        Jedis connection = getConnection();
        try
        {

            // first open a pipeline
            // Create a hashset and populate data into it
            Pipeline pipeLine = connection.pipelined();
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
                rowKey = prepareCompositeKey(entityMetadata, metaModel, id);
            }
            else
            {
                rowKey = PropertyAccessorHelper.getString(entity, (Field) entityMetadata.getIdAttribute()
                        .getJavaMember());
            }

            String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);

            connection.hmset(getEncodedBytes(hashKey), wrapper.getColumns());

            // Add row key to list(Required for wild search over table).
            connection.zadd(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()),
                    getDouble(rowKey), rowKey);

            // Add inverted indexes for column based search.
            addIndex(connection, wrapper, rowKey);

            // Add row-key as inverted index as well needed for multiple clause
            // search with key and non row key.
            connection
                    .zadd(getHashKey(
                            entityMetadata.getTableName(),
                            getHashKey(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), rowKey)),
                            getDouble(rowKey), rowKey);
            //
            pipeLine.sync(); // send I/O.. as persist call. so no need to read
                             // response?
        }
        finally
        {
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
        Jedis connection = getConnection();
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
     * @param clazz                     entity class
     * @param key                       row key 
     * @param connection                connection instance.
     * @param fields                    fields.
     * @return                          entity instance.
     * @throws InstantiationException   throws in case of runtime exception
     * @throws IllegalAccessException   throws in case of runtime exception 
     */
    private Object fetch(Class clazz, Object key, Jedis connection, byte[][] fields) throws InstantiationException,
            IllegalAccessException
    {
        Object result;

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz);

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        String rowKey = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            rowKey = prepareCompositeKey(entityMetadata, metaModel, key);
        }
        else
        {
            rowKey = PropertyAccessorHelper.getString(key);
        }

        String hashKey = getHashKey(entityMetadata.getTableName(), rowKey);

        try
        {
            Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();

            // IF it is for selective columns
            if (fields != null)
            {
                List<byte[]> fieldValues = connection.hmget(getEncodedBytes(hashKey), fields);

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
                columns = connection.hgetAll(getEncodedBytes(hashKey));
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

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class, java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        Jedis connection = getConnection();
        connection.pipelined();
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
        if(settings != null)
        {
            settings.clear();
            settings = null;
        }
        reader=null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object, java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        Jedis connection = null;
        try
        {
            connection = getConnection();
            Pipeline pipeLine = connection.pipelined();

            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
            AttributeWrapper wrapper = wrap(entityMetadata, entity);

            Set<byte[]> columnNames = wrapper.columns.keySet();

            String rowKey = null;

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                rowKey = prepareCompositeKey(entityMetadata, metaModel, pKey);
            }
            else
            {
                rowKey = PropertyAccessorHelper.getString(entity, (Field) entityMetadata.getIdAttribute()
                        .getJavaMember());
            }

            for (byte[] name : columnNames)
            {
                connection.hdel(getHashKey(entityMetadata.getTableName(), rowKey),
                        PropertyAccessorFactory.STRING.fromBytes(String.class, name));
            }

            // Delete relation values.

            deleteRelation(connection, entityMetadata, rowKey);

            // Delete inverted indexes.
            unIndex(connection, wrapper, rowKey);

            connection.zrem(
                    getHashKey(entityMetadata.getTableName(),
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), rowKey);

            pipeLine.sync();
        }
        finally
        {
            onCleanup(connection);
        }
    }

    /**
     * On delete relation.
     * 
     * @param connection       connection instance.
     * @param entityMetadata   entity metadata.
     * @param rowKey           row key.
     */
    private void deleteRelation(Jedis connection, EntityMetadata entityMetadata, String rowKey)
    {
        List<String> relations = entityMetadata.getRelationNames();

        if (relations != null)
        {
            for (String relation : relations)
            {
                connection.hdel(getHashKey(entityMetadata.getTableName(), rowKey), relation);
            }

        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String tableName = joinTableData.getJoinTableName();
        String inverseJoinColumn = joinTableData.getInverseJoinColumnName();
        String joinColumn = joinTableData.getJoinColumnName();

        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();
        Jedis connection = null;
        /**
         * Example: join table : PERSON_ADDRESS join column : PERSON_ID (1_p)
         * inverse join column : ADDRESS_ID (1_a) store in REDIS:
         * PERSON_ADDRESS:1_p_1_a PERSON_ID 1_p ADDRESS_ID 1_a
         */
        // String rowKey =
        try
        {
            connection = getConnection();
            Pipeline pipeline = connection.pipelined();
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
                    connection.hmset(getEncodedBytes(redisKey), redisFields);
                    // add index
                    connection.zadd(getHashKey(tableName, inverseJoinKeyAsStr),
                            getDouble(inverseJoinKeyAsStr), redisKey);
                    connection.zadd(getHashKey(tableName, joinKeyAsStr),
                            getDouble(joinKeyAsStr), redisKey);
                    redisFields.clear();
                }

            }
            pipeline.sync();
        }
        finally
        {
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
            Object pKeyColumnValue)
    {
        Jedis connection = null;

        List results = new ArrayList();

        try
        {
            connection = getConnection();

            Pipeline pipeLine = connection.pipelined();
            String valueAsStr = PropertyAccessorHelper.getString(pKeyColumnValue);

            Double score = getDouble(valueAsStr);
            Set<String> resultKeys = connection.zrangeByScore(getHashKey(tableName, valueAsStr), score, score);

            for (String hashKey : resultKeys)
            {
                List columnValues = connection.hmget(hashKey, columnName);

                pipeLine.syncAndReturnAll();
                if (columnValues != null && !columnValues.isEmpty())
                {
                    results.addAll(columnValues); // Currently returning list of
                                                  // string as known issue with
                                                  // joint table concept!
                }

            }

            // return connection.hmget(getEncodedBytes(redisKey),
            // getEncodedBytes(columnName));
            return results;
        }
        finally
        {
            onCleanup(connection);
        }
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        Jedis connection = null;

        try
        {
            connection = getConnection();
            String valueAsStr = PropertyAccessorHelper.getString(columnValue);

            Set<String> results = connection.zrangeByScore(getHashKey(tableName, columnName), getDouble(valueAsStr),getDouble(valueAsStr));
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

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        Jedis connection = null;
        try
        {
            connection = getConnection();
            Pipeline pipeLine = connection.pipelined();
            String valueAsStr = PropertyAccessorHelper.getString(columnValue);
            Double score = getDouble(valueAsStr);
            Set<String> results = connection.zrangeByScore(getHashKey(tableName, valueAsStr), score, score);

            if (results != null)
            {
                for (String rowKey : results)
                {
                    // byte[] hashKey = getEncodedBytes(getHashKey(tableName,
                    // rowKey));
                    Map<byte[], byte[]> columns = connection.hgetAll(getEncodedBytes(rowKey));

                    for (byte[] column : columns.keySet()) // delete each
                                                           // column(e.g.
                    // field)
                    {
                        // connection.get(key)
                        connection.hdel(getEncodedBytes(rowKey), column); // delete
                                                                          // record
                        String colName = PropertyAccessorFactory.STRING.fromBytes(String.class, columns.get(column));
                        connection.zrem(getHashKey(tableName, colName), rowKey); // delete
                                                                                 // inverted
                                                                                 // index.
                    }
                }

            }
            pipeLine.sync();
        }
        finally
        {
            onCleanup(connection);
        }
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        Object[] ids = findIdsByColumn(entityMetadata.getSchema(), entityMetadata.getTableName(),
                ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), colName, colValue,
                entityClazz);
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
     *  To supply configurations for jedis connection.  
     */
    public void setConfig(Map<String,Object> configurations)
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
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        // TODO Auto-generated method stub
        return 0;
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
    private void addIndex(final Jedis connection, final AttributeWrapper wrapper, final String rowKey)
    {
        Set<String> indexKeys = wrapper.getIndexes().keySet();

        for (String idx_Name : indexKeys)
        {
            connection.zadd(idx_Name, wrapper.getIndexes().get(idx_Name), rowKey);
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
    private void unIndex(final Jedis connection, final AttributeWrapper wrapper, final String member)
    {
        Set<String> keys = wrapper.getIndexes().keySet();
        for (String key : keys)
        {
            connection.zrem(key, member);
        }
    }

    /**
     * On release connection.
     * 
     * @param connection
     *            redis connection instance.
     */
    private void onCleanup(Jedis connection)
    {
        if (connection != null)
        {
            if(settings != null)
            {
                connection.configResetStat();
            }
            factory.releaseConnection(connection);
        }
    }

    /**
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
    private String prepareCompositeKey(final EntityMetadata m, final MetamodelImpl metaModel, final Object compositeKey)
    {
//        EmbeddableType keyObject = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());

        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();

        StringBuilder stringBuilder = new StringBuilder();
        for (Field f : fields)
        {
//            Attribute compositeColumn = keyObject.getAttribute(f.getName());
            try
            {
                String fieldValue = PropertyAccessorHelper.getString(compositeKey, f); // field
                                                                                       // value
                stringBuilder.append(fieldValue);
                stringBuilder.append(COMPOSITE_KEY_SEPERATOR);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Error during persist, Caused by:", e);
                throw new PersistenceException(e);
            }

        }

        if (stringBuilder.length() > 0)
        {
            stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMPOSITE_KEY_SEPERATOR));
        }
        return stringBuilder.toString();
    }

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
                                             // but possible negative scenario
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
        if (embeddedAttrib == null)
        {
            name = getEncodedBytes(((AbstractAttribute) attrib).getJPAColumnName());
        }
        else
        {
            name = getEncodedBytes(getHashKey(embeddedAttrib.getName(), ((AbstractAttribute) attrib).getJPAColumnName()));
        }
        // add column name as key and value as value
        wrapper.addColumn(name, value);
        // // {tablename:columnname,hashcode} for value
        wrapper.addIndex(getHashKey(entityMetadata.getTableName(), ((AbstractAttribute) attrib).getJPAColumnName()),
                getDouble(valueAsStr));

        wrapper.addIndex(
                getHashKey(entityMetadata.getTableName(),
                        getHashKey(((AbstractAttribute) attrib).getJPAColumnName(), valueAsStr)), getDouble(valueAsStr));

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
        Jedis connection = null;
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
                    connection.zinterstore(destStore, keySets.toArray(new String[] {}));
                }
                else
                {
                    connection.zunionstore(destStore, keySets.toArray(new String[] {}));
                }

                rowKeys = connection.zrange(destStore, 0, -1);

                // delete intermediate store after find.
                connection.del(destStore);
                //
                // means it is a query over sorted set.
            }
            else if (queryParameter.isByRange())
            {
                // means query over a single sorted set with range
                Map<String, Double> minimum = queryParameter.getMin();
                Map<String, Double> maximum = queryParameter.getMax();

                String column = minimum.keySet().iterator().next();

                rowKeys = connection.zrangeByScore(getHashKey(entityMetadata.getTableName(), column),
                        minimum.get(column), maximum.get(column));

            }
            else if (queryParameter.isById())
            {
                Map<String, Object> fieldSets = queryParameter.getFields();

                results = findAllColumns(entityClazz, (byte[][]) (queryParameter.getColumns() != null ? queryParameter
                        .getColumns().toArray() : null), fieldSets.values().toArray());
                return results;
            }
            else if(queryParameter.getFields() != null)  
            {
                Set<String> columns =  queryParameter.getFields().keySet();
                
                for(String column : columns)
                {
                    // ideally it will always be 1 value in map, else it will go it queryParameter.getClause() will not be null!
                    Double value = getDouble(PropertyAccessorHelper.getString(queryParameter.getFields().get(column)));
                    rowKeys = connection.zrangeByScore(getHashKey(entityMetadata.getTableName(), column),
                            value, value);
                    
                }
                

            } else
            {
                rowKeys = new HashSet<String>(connection.zrange(
                        getHashKey(entityMetadata.getTableName(),
                                ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName()), 0, -1));
            }

            // fetch fr
            for (String k : rowKeys)
            {

                Object record = fetch(entityClazz, k, connection,
                        (queryParameter.getColumns() != null ? queryParameter.getColumns().toArray(new byte[][]{}) : null));
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
            if (connection != null)
            {
                factory.releaseConnection(connection);
            }
        }

        return results;
    }

    private <E> List<E> findAllColumns(Class<E> entityClass, byte[][] columns, Object... keys)
    {
        Jedis connection = getConnection();
//        connection.co
        connection.pipelined();
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
    }

    
    /**
     * Returns jedis connection.
     * 
     * @return jedis resource.
     */
    private Jedis getConnection()
    {
        Jedis connection = factory.getConnection();
        if(settings != null)
        {
            for(String key : settings.keySet())
            {
                connection.configSet(key, settings.get(key).toString());
            }
        }
        return connection;
    }
}
