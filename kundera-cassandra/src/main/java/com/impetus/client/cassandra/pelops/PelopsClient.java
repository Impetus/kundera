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

package com.impetus.client.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.client.cassandra.pelops.PelopsDataHandler.ThriftRow;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient extends ClientBase implements Client<CassQuery>
{

    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private PelopsDataHandler handler;

    /** The reader. */
    private EntityReader reader;

    /** The timestamp. */
    private long timestamp;

    /**
     * default constructor.
     * 
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param persistenceUnit
     *            the persistence unit
     */
    public PelopsClient(IndexManager indexManager, EntityReader reader, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.handler = new PelopsDataHandler();
        this.reader = reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String)
     */
    @Override
    public final Object find(Class entityClass, Object rowId)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        return find(entityClass, entityMetadata, rowId != null ? rowId.toString() : null, relationNames);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String[])
     */
    @Override
    public final <E> List<E> findAll(Class<E> entityClass, Object... rowIds)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        List<E> results = new ArrayList<E>();
        results = find(entityClass, entityMetadata.getRelationNames(), entityMetadata.getRelationNames() != null
                && !entityMetadata.getRelationNames().isEmpty(), entityMetadata, rowIds);
        return results.isEmpty() ? null : results;
    }

    /**
     * Method to return list of entities for given below attributes:.
     * 
     * @param entityClass
     *            entity class
     * @param relationNames
     *            relation names
     * @param isWrapReq
     *            true, in case it needs to populate enhance entity.
     * @param metadata
     *            entity metadata.
     * @param rowIds
     *            array of row key s
     * @return list of wrapped entities.
     */
    public final List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds)
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        // selector.

        // PelopsDataHandler handler = new PelopsDataHandler(this);

        List entities = null;
        try
        {
            // TODO
            entities = handler.fromThriftRow(selector, entityClass, metadata, relationNames, isWrapReq,
                    consistencyLevel, rowIds);
        }
        catch (Exception e)
        {
            throw new KunderaException(e);
        }

        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> superColumnMap)
    {
        List<E> entities = null;
        try
        {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
            entities = new ArrayList<E>();
            for (String superColumnName : superColumnMap.keySet())
            {
                String entityId = superColumnMap.get(superColumnName);
                List<SuperColumn> superColumnList = loadSuperColumns(entityMetadata.getSchema(),
                        entityMetadata.getTableName(), entityId,
                        new String[] { superColumnName.substring(0, superColumnName.indexOf("|")) });
                E e = (E) handler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                        new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
                entities.add(e);
            }
        }
        catch (Exception e)
        {
            throw new KunderaException(e);
        }
        return entities;
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
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
        rowDeletor.deleteRow(metadata.getTableName(), pKey.toString(), consistencyLevel);
        getIndexManager().remove(metadata, entity, pKey.toString());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public final void close()
    {
        this.indexManager.flush();
        this.handler = null;
        closed = true;

    }

    /**
     * Persists records into Join Table
     */
    public void persistJoinTable(JoinTableData joinTableData)
    {

        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit());
        Mutator mutator = Pelops.createMutator(poolName);

        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);

            List<Column> columns = new ArrayList<Column>();

            for (Object value : values)
            {
                Column column = new Column();
                column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName + "_" + (String) value));
                column.setValue(PropertyAccessorFactory.STRING.toBytes((String) value));
                column.setTimestamp(System.currentTimeMillis());

                columns.add(column);
            }

            createIndexesOnColumns(joinTableName, poolName, columns);
            String pk = (String) key;

            mutator.writeColumns(joinTableName, Bytes.fromUTF8(pk), Arrays.asList(columns.toArray(new Column[0])));
            mutator.execute(consistencyLevel);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#getForeignKeysFromJoinTable(java.lang
     * .String, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public <E> List<E> getColumnsById(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            String parentId)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        List<Column> columns = selector.getColumnsFromRow(joinTableName, Bytes.fromUTF8(parentId),
                Selector.newColumnsPredicateAll(true, 10), consistencyLevel);

        // PelopsDataHandler handler = new PelopsDataHandler(this);
        List<E> foreignKeys = handler.getForeignKeysFromJoinTable(inverseJoinColumnName, columns);
        return foreignKeys;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String tableName, String pKeyName, String columnName, Object columnValue,
            Class entityClazz)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        String childIdStr = (String) columnValue;

        IndexClause ix = Selector.newIndexClause(
                Bytes.EMPTY,
                10000,
                Selector.newIndexExpression(columnName + "_" + childIdStr, IndexOperator.EQ,
                        Bytes.fromByteArray(childIdStr.getBytes())));

        Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(tableName, ix, slicePredicate, consistencyLevel);

        List<Object> rowKeys = new ArrayList<Object>();

        // iterate through complete map and
        Iterator<Bytes> rowIter = qResults.keySet().iterator();
        while (rowIter.hasNext())
        {
            Bytes rowKey = rowIter.next();

            PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(metadata.getIdColumn()
                    .getField());
            Object value = accessor.fromBytes(metadata.getIdColumn().getField().getClass(), rowKey.toByteArray());

            rowKeys.add(value);
        }

        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.Object)
     */
    // Incorrect
    public void deleteByColumn(String tableName, String columnName, Object columnValue)
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
        rowDeletor.deleteRow(tableName, columnValue.toString(), consistencyLevel);
    }

    /**
     * Find.
     * 
     * @param ixClause
     *            the ix clause
     * @param m
     *            the m
     * @param isRelation
     *            the is relation
     * @param relations
     *            the relations
     * @return the list
     */
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations)
    {
        // ixClause can be 0,1 or more!
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        List<Object> entities = null;
        if (ixClause.isEmpty())
        {
            Map<Bytes, List<Column>> qResults = selector.getColumnsFromRows(m.getTableName(),
                    selector.newKeyRange("", "", 100), slicePredicate, consistencyLevel);
            entities = new ArrayList<Object>(qResults.size());
            populateData(m, qResults, entities, isRelation, relations);
        }
        else
        {
            entities = new ArrayList<Object>();
            for (IndexClause ix : ixClause)
            {
                Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate,
                        consistencyLevel);
                // iterate through complete map and
                populateData(m, qResults, entities, isRelation, relations);
            }
        }
        return entities;
    }

    /**
     * Find by range.
     * 
     * @param minVal
     *            the min val
     * @param maxVal
     *            the max val
     * @param m
     *            the m
     * @param isWrapReq
     *            the is wrap req
     * @param relations
     *            the relations
     * @return the list
     * @throws Exception
     *             the exception
     */
    public List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations)
            throws Exception
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        List<Object> entities = null;

        List<KeySlice> keys = selector.getKeySlices(new ColumnParent(m.getTableName()),
                selector.newKeyRange(Bytes.fromByteArray(minVal), Bytes.fromByteArray(maxVal), 10000), slicePredicate,
                consistencyLevel);

        List<String> superColumnNames = m.getEmbeddedColumnFieldNames();

        List results = null;
        if (keys != null)
        {
            results = new ArrayList(keys.size());
            for (KeySlice key : keys)
            {
                List<ColumnOrSuperColumn> columns = key.getColumns();
                byte[] rowKey = key.getKey();

                if (!superColumnNames.isEmpty())
                {
                    List<SuperColumn> superColumns = new ArrayList<SuperColumn>(columns.size());

                    for (ColumnOrSuperColumn supCol : columns)
                    {
                        superColumns.add(supCol.getSuper_column());
                    }

                    Object r = handler.fromSuperColumnThriftRow(m.getEntityClazz(), m, handler.new ThriftRow(
                            new String(rowKey), m.getTableName(), null, superColumns), relations, isWrapReq);
                    results.add(r);
                    // List<SuperColumn> superCol = columns.
                }
                else
                {
                    List<Column> cols = new ArrayList<Column>(columns.size());
                    for (ColumnOrSuperColumn supCol : columns)
                    {
                        cols.add(supCol.getColumn());
                    }

                    Object r = handler.fromColumnThriftRow(m.getEntityClazz(), m, handler.new ThriftRow(new String(
                            rowKey), m.getTableName(), cols, null), relations, isWrapReq);
                    results.add(r);
                }
            }
        }

        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public List<Object> findByRelation(String colName, String colValue, Class clazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(clazz);
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        List<Object> entities = null;
        IndexClause ix = Selector.newIndexClause(Bytes.EMPTY, 10000,
                Selector.newIndexExpression(colName, IndexOperator.EQ, Bytes.fromByteArray(colValue.getBytes())));
        Map<Bytes, List<Column>> qResults;
        try
        {
            qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate, consistencyLevel);
        }
        catch (PelopsException e)
        {
            log.warn(e.getMessage());
            return entities;
        }
        entities = new ArrayList<Object>(qResults.size());
        // iterate through complete map and
        populateData(m, qResults, entities, false, null);

        return entities;
    }

    /**
     * Find.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param conditions
     *            the conditions
     * @return the list
     */
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions)
    {

        return (List<EnhanceEntity>) find(conditions, m, true, relationNames);
    }

    /**
     * Populates foreign key as column.
     * 
     * @param rlName
     *            relation name
     * @param rlValue
     *            relation value
     * @param timestamp
     *            the timestamp
     * @return the column
     * @throws PropertyAccessException
     *             the property access exception
     */
    private Column populateFkey(String rlName, String rlValue, long timestamp) throws PropertyAccessException
    {
        Column col = new Column();
        col.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        col.setValue(rlValue.getBytes());
        col.setTimestamp(timestamp);
        return col;
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

    /**
     * Method to execute cql query and return back entity/enhance entities.
     * 
     * @param cqlQuery
     *            cql query to be executed.
     * @param clazz
     *            entity class.
     * @param relationalField
     *            collection for relational fields.
     * @return list of objects.
     * 
     */
    public List executeQuery(String cqlQuery, Class clazz, List<String> relationalField)
    {
        IThriftPool thrift = Pelops.getDbConnPool(PelopsUtils.generatePoolName(getPersistenceUnit()));
        // thrift.get
        IPooledConnection connection = thrift.getConnection();
        try
        {
            org.apache.cassandra.thrift.Cassandra.Client thriftClient = connection.getAPI();

            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz);
            CqlResult result = null;
            List returnedEntities = null;
            try
            {
                result = thriftClient.execute_cql_query(ByteBufferUtil.bytes(cqlQuery),
                        org.apache.cassandra.thrift.Compression.NONE);
                if (result != null && (result.getRows() != null || result.getRowsSize() > 0))
                {
                    returnedEntities = new ArrayList<Object>(result.getRowsSize());
                    Iterator<CqlRow> iter = result.getRowsIterator();
                    while (iter.hasNext())
                    {
                        CqlRow row = iter.next();
                        String rowKey = Bytes.toUTF8(row.getKey());

                        ThriftRow thriftRow = handler.new ThriftRow(rowKey, entityMetadata.getTableName(),
                                row.getColumns(), null);

                        Object entity = handler.fromColumnThriftRow(clazz, entityMetadata, thriftRow, relationalField,
                                relationalField != null && !relationalField.isEmpty());
                        returnedEntities.add(entity);
                    }
                }
            }
            catch (InvalidRequestException e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            catch (UnavailableException e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            catch (TimedOutException e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            catch (SchemaDisagreementException e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            catch (TException e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            catch (Exception e)
            {
                log.error("Error while executing native CQL query Caused by:" + e.getLocalizedMessage());
                throw new PersistenceException(e);
            }
            return returnedEntities;
        }
        finally
        {
            try
            {
                if (connection != null)
                {
                    connection.release();
                }
            }
            catch (Exception e)
            {
                log.warn("Releasing connection for native CQL query failed", e);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<CassQuery> getQueryImplementor()
    {
        return CassQuery.class;
    }

    @Override
    protected void onPersist(EntityMetadata metadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        PelopsDataHandler.ThriftRow tf = null;
        try
        {
            tf = populateTfRow(entity, id.toString(), metadata);
        }
        catch (Exception e)
        {
            log.error("Error during persist, Caused by:" + e.getMessage());
            throw new KunderaException(e);
        }

        addRelationsToThriftRow(metadata, tf, rlHolders);

        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));

        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if (thriftColumns != null && !thriftColumns.isEmpty())
        {
            // Bytes.fromL
            mutator.writeColumns(metadata.getTableName(), Bytes.fromUTF8(tf.getId()),
                    Arrays.asList(tf.getColumns().toArray(new Column[0])));
        }

        if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
        {
            for (SuperColumn sc : thriftSuperColumns)
            {
                mutator.writeSubColumns(metadata.getTableName(), tf.getId(), Bytes.toUTF8(sc.getName()),
                        sc.getColumns());

            }

        }

        mutator.execute(consistencyLevel);
        tf = null;
    }

    /**
     * Populate tf row.
     * 
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param metadata
     *            the metadata
     * @return the pelops data handler n. thrift row
     * @throws Exception
     *             the exception
     */
    private PelopsDataHandler.ThriftRow populateTfRow(Object entity, String id, EntityMetadata metadata)
            throws Exception
    {

        String columnFamily = metadata.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        // PelopsDataHandler handler = dataHandler != null ? dataHandler : new
        // PelopsDataHandler(;
        PelopsDataHandler.ThriftRow tf = handler.toThriftRow(entity, id, metadata, columnFamily);
        timestamp = handler.getTimestamp();
        return tf;
    }

    /**
     * Adds relation foreign key values as thrift column/ value to thrift row
     * 
     * @param metadata
     * @param tf
     * @param relations
     */
    private void addRelationsToThriftRow(EntityMetadata metadata, PelopsDataHandler.ThriftRow tf,
            List<RelationHolder> relations)
    {
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                String linkName = rh.getRelationName();
                String linkValue = rh.getRelationValue();

                if (linkName != null && linkValue != null)
                {
                    if (metadata.getEmbeddedColumnsAsList().isEmpty())
                    {
                        Column col = populateFkey(linkName, linkValue, timestamp);
                        tf.addColumn(col);
                    }
                    else
                    {
                        SuperColumn superColumn = new SuperColumn();
                        superColumn.setName(linkName.getBytes());
                        Column column = populateFkey(linkName, linkValue, timestamp);
                        superColumn.addToColumns(column);
                        tf.addSuperColumn(superColumn);
                    }
                }
            }
        }
    }

    /**
     * Find.
     * 
     * @param clazz
     *            the clazz
     * @param metadata
     *            the metadata
     * @param rowId
     *            the row id
     * @param relationNames
     *            the relation names
     * @return the object
     */
    private final Object find(Class<?> clazz, EntityMetadata metadata, Object rowId, List<String> relationNames)
    {

        List<Object> result = null;
        try
        {
            result = (List<Object>) find(clazz, relationNames, relationNames != null, metadata,
                    rowId != null ? rowId.toString() : null);
        }
        catch (Exception e)
        {
            log.error("Error on retrieval" + e.getMessage());
            throw new PersistenceException(e.getMessage());
        }

        return result != null & !result.isEmpty() ? result.get(0) : null;
    }

    /**
     * Populate data.
     * 
     * @param m
     *            the m
     * @param qResults
     *            the q results
     * @param entities
     *            the entities
     * @param isRelational
     *            the is relational
     * @param relationNames
     *            the relation names
     */
    private void populateData(EntityMetadata m, Map<Bytes, List<Column>> qResults, List<Object> entities,
            boolean isRelational, List<String> relationNames)
    {
        Iterator<Bytes> rowIter = qResults.keySet().iterator();
        while (rowIter.hasNext())
        {
            Bytes rowKey = rowIter.next();
            List<Column> columns = qResults.get(rowKey);
            try
            {
                Object e = handler.fromColumnThriftRow(m.getEntityClazz(), m,
                        handler.new ThriftRow(Bytes.toUTF8(rowKey.toByteArray()), m.getTableName(), columns, null),
                        relationNames, isRelational);
                entities.add(e);
            }
            catch (IllegalStateException e)
            {
                throw new KunderaException(e);
            }
            catch (Exception e)
            {
                throw new KunderaException(e);
            }
        }
    }

    /**
     * Creates secondary indexes on columns if not already created.
     * 
     * @param tableName
     *            Column family name
     * @param poolName
     *            Pool Name
     * @param columns
     *            List of columns
     */
    private void createIndexesOnColumns(String tableName, String poolName, List<Column> columns)
    {
        String keyspace = Pelops.getDbConnPool(poolName).getKeyspace();
        try
        {
            Cassandra.Client api = Pelops.getDbConnPool(poolName).getConnection().getAPI();
            KsDef ksDef = api.describe_keyspace(keyspace);
            List<CfDef> cfDefs = ksDef.getCf_defs();

            // Column family definition on which secondary index creation is
            // required
            CfDef columnFamilyDefToUpdate = null;
            boolean isUpdatable = false;
            // boolean isNew=false;
            for (CfDef cfDef : cfDefs)
            {
                if (cfDef.getName().equals(tableName))
                {
                    columnFamilyDefToUpdate = cfDef;
                    // isNew=false;
                    break;
                }
            }

            // //create a column family, in case it is not already available.
            // if(columnFamilyDefToUpdate == null)
            // {
            // isNew = true;
            // columnFamilyDefToUpdate = new CfDef(keyspace, tableName);
            // ksDef.addToCf_defs(columnFamilyDefToUpdate);
            // }

            // Get list of indexes already created
            List<ColumnDef> columnMetadataList = columnFamilyDefToUpdate.getColumn_metadata();
            List<String> indexList = new ArrayList<String>();

            if (columnMetadataList != null)
            {
                for (ColumnDef columnDef : columnMetadataList)
                {
                    indexList.add(Bytes.toUTF8(columnDef.getName()));
                }
                // need to set them to null else it is giving problem on update
                // column family and trying to add again existing indexes.
                // columnFamilyDefToUpdate.column_metadata = null;
            }

            // Iterate over all columns for creating secondary index on them
            for (Column column : columns)
            {

                ColumnDef columnDef = new ColumnDef();

                columnDef.setName(column.getName());
                columnDef.setValidation_class("UTF8Type");
                columnDef.setIndex_type(IndexType.KEYS);

                // String indexName =
                // PelopsUtils.getSecondaryIndexName(tableName, column);

                // Add secondary index only if it's not already created
                // (if already created, it would be there in column family
                // definition)
                if (!indexList.contains(Bytes.toUTF8(column.getName())))
                {
                    isUpdatable = true;
                    columnFamilyDefToUpdate.addToColumn_metadata(columnDef);
                }
            }

            // Finally, update column family with modified column family
            // definition
            if (isUpdatable)
            {
                api.system_update_column_family(columnFamilyDefToUpdate);
            }// } else
             // {
             // api.system_add_column_family(columnFamilyDefToUpdate);
             // }

        }
        catch (InvalidRequestException e)
        {
            log.warn("Could not create secondary index on column family " + tableName + ".Details:" + e.getMessage());

        }
        catch (SchemaDisagreementException e)
        {
            log.warn("Could not create secondary index on column family " + tableName + ".Details:" + e.getMessage());

        }
        catch (TException e)
        {
            log.warn("Could not create secondary index on column family " + tableName + ".Details:" + e.getMessage());

        }
        catch (NotFoundException e)
        {
            log.warn("Could not create secondary index on column family " + tableName + ".Details:" + e.getMessage());

        }
        catch (PropertyAccessException e)
        {
            log.warn("Could not create secondary index on column family " + tableName + ".Details:" + e.getMessage());

        }
    }

    /**
     * Load super columns.
     * 
     * @param keyspace
     *            the keyspace
     * @param columnFamily
     *            the column family
     * @param rowId
     *            the row id
     * @param superColumnNames
     *            the super column names
     * @return the list
     */
    private final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames)
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                consistencyLevel);
    }

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    private final boolean isOpen()
    {
        return !closed;
    }

    public void setConsistencyLevel(ConsistencyLevel cLevel)
    {
        this.consistencyLevel = cLevel;
    }
}