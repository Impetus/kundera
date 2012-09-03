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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
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

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient extends CassandraClientBase implements Client<CassQuery>
{

    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private PelopsDataHandler dataHandler;

    /** Handler for Inverted indexing */
    private InvertedIndexHandler invertedIndexHandler;

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
        this.dataHandler = new PelopsDataHandler();
        this.invertedIndexHandler = new PelopsInvertedIndexHandler();
        this.reader = reader;
    }

    @Override
    public final Object find(Class entityClass, Object rowId)
    {
        return super.find(entityClass, rowId);
    }

    @Override
    public final <E> List<E> findAll(Class<E> entityClass, Object... rowIds)
    {
        return super.findAll(entityClass, rowIds);
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
    @Override
    public final List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds)
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        List entities = null;
        try
        {
            entities = dataHandler.fromThriftRow(entityClass, metadata, relationNames, isWrapReq, consistencyLevel,
                    rowIds);
        }
        catch (Exception e)
        {
            throw new KunderaException(e);
        }

        return entities;
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        if (metadata.isCounterColumnType())
        {
            deleteRecordFromCounterColumnFamily(pKey, metadata, consistencyLevel);
        }
        else
        {
            RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
            rowDeletor.deleteRow(metadata.getTableName(),
                    CassandraUtilities.toBytes(pKey, metadata.getIdAttribute().getJavaType()), consistencyLevel);

        }

        // Delete from Lucene if applicable
        getIndexManager().remove(metadata, entity, pKey.toString());

        // Delete from Inverted Index if applicable
        invertedIndexHandler.delete(entity, metadata, consistencyLevel);

    }

    @Override
    public final void close()
    {
        this.indexManager.flush();
        this.dataHandler = null;
        this.invertedIndexHandler = null;
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
                column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName + Constants.JOIN_COLUMN_NAME_SEPARATOR + value.toString()));
                // column.setValue(PropertyAccessorFactory.STRING.toBytes((String)
                // value));
                column.setValue(PropertyAccessorHelper.getBytes(value));
                column.setTimestamp(System.currentTimeMillis());

                columns.add(column);
            }

            createIndexesOnColumns(joinTableName, poolName, columns);
            Object pk = key;

            mutator.writeColumns(joinTableName, Bytes.fromByteArray(PropertyAccessorHelper.getBytes(pk)),
                    Arrays.asList(columns.toArray(new Column[0])));
            mutator.execute(consistencyLevel);
        }

    }

    @Override
    public <E> List<E> getColumnsById(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            Object parentId)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
//        List<Column> columns = selector.getColumnsFromRow(joinTableName, Bytes.fromUTF8(parentId.toString()),
//                Selector.newColumnsPredicateAll(true, 10), consistencyLevel);
        List<Column> columns = selector.getColumnsFromRow(joinTableName, Bytes.fromByteArray(PropertyAccessorHelper.getBytes(parentId)),
                Selector.newColumnsPredicateAll(true, 10), consistencyLevel);

        List<E> foreignKeys = dataHandler.getForeignKeysFromJoinTable(inverseJoinColumnName, columns);
        return foreignKeys;
    }

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
                Selector.newIndexExpression(columnName + Constants.JOIN_COLUMN_NAME_SEPARATOR + childIdStr, IndexOperator.EQ,
                        Bytes.fromByteArray(childIdStr.getBytes())));

        Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(tableName, ix, slicePredicate, consistencyLevel);

        List<Object> rowKeys = new ArrayList<Object>();

        // iterate through complete map and
        Iterator<Bytes> rowIter = qResults.keySet().iterator();
        while (rowIter.hasNext())
        {
            Bytes rowKey = rowIter.next();

            PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor((Field) metadata
                    .getIdAttribute().getJavaMember());
            Object value = accessor.fromBytes(metadata.getIdAttribute().getJavaType(), rowKey.toByteArray());

            rowKeys.add(value);
        }

        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }
        return null;
    }

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

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return super.find(entityClass, embeddedColumnMap, dataHandler);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class clazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(clazz);
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        List<Object> entities = null;
        IndexClause ix = Selector.newIndexClause(
                Bytes.EMPTY,
                10000,
                Selector.newIndexExpression(colName, IndexOperator.EQ,
                        Bytes.fromByteArray(PropertyAccessorHelper.getBytes(colValue))));
        Map<Bytes, List<Column>> qResults;
        try
        {
            qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate, consistencyLevel);
        }
        catch (PelopsException e)
        {
            log.info(e.getMessage());
            return entities;
        }
        entities = new ArrayList<Object>(qResults.size());
        // iterate through complete map and
        populateData(m, qResults, entities, false, null, dataHandler);

        return entities;
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

    @Override
    public Class<CassQuery> getQueryImplementor()
    {
        return CassQuery.class;
    }

    @Override
    protected void onPersist(EntityMetadata metadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        // check for counter column
        if (isUpdate && metadata.isCounterColumnType())
        {
            throw new UnsupportedOperationException(" Merge is not permitted on counter column! ");
        }

        ThriftRow tf = null;
        try
        {
            String columnFamily = metadata.getTableName();
            tf = dataHandler.toThriftRow(entity, id, metadata, columnFamily);
            timestamp = System.currentTimeMillis();
        }
        catch (Exception e)
        {
            log.error("Error during persist, Caused by:" + e.getMessage());
            throw new KunderaException(e);
        }

        addRelationsToThriftRow(metadata, tf, rlHolders);

        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));
        if (metadata.isCounterColumnType())
        {
            List<CounterColumn> thriftCounterColumns = tf.getCounterColumns();
            List<CounterSuperColumn> thriftCounterSuperColumns = tf.getCounterSuperColumns();
            if (thriftCounterColumns != null && !thriftCounterColumns.isEmpty())
            {
                mutator.writeCounterColumns(metadata.getTableName(),
                        CassandraUtilities.toBytes(tf.getId(), tf.getId().getClass()),
                        Arrays.asList(tf.getCounterColumns().toArray(new CounterColumn[0])));
            }

            if (thriftCounterSuperColumns != null && !thriftCounterSuperColumns.isEmpty())
            {
                for (CounterSuperColumn sc : thriftCounterSuperColumns)
                {
                    mutator.writeSubCounterColumns(metadata.getTableName(),
                            CassandraUtilities.toBytes(tf.getId(), tf.getId().getClass()),
                            Bytes.fromByteArray(sc.getName()), sc.getColumns());
                }
            }
        }
        else
        {
            List<Column> thriftColumns = tf.getColumns();
            List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
            if (thriftColumns != null && !thriftColumns.isEmpty())
            {
                // Bytes.fromL
                mutator.writeColumns(metadata.getTableName(),
                        CassandraUtilities.toBytes(tf.getId(), tf.getId().getClass()),
                        Arrays.asList(tf.getColumns().toArray(new Column[0])));
            }

            if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
            {
                for (SuperColumn sc : thriftSuperColumns)
                {
                    mutator.writeSubColumns(metadata.getTableName(),
                            CassandraUtilities.toBytes(tf.getId(), tf.getId().getClass()),
                            Bytes.fromByteArray(sc.getName()), sc.getColumns());
                }
            }
        }

        mutator.execute(consistencyLevel);
        tf = null;
    }

    /**
     * Indexes @Embedded and @ElementCollection objects of this entity to a
     * separate column family
     */
    @Override
    protected void indexNode(Node node, EntityMetadata entityMetadata)
    {
        // Index to lucene if applicable
        super.indexNode(node, entityMetadata);

        // Write to inverted index table if applicable
        invertedIndexHandler.write(node, entityMetadata, getPersistenceUnit(), consistencyLevel, dataHandler);
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

    @Override
    public final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames)
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>();
        rowKeys.add(ByteBuffer.wrap(rowId.getBytes()));

        // Pelops.getDbConnPool("").getConnection().getAPI().

        // selector.getColumnOrSuperColumnsFromRows(new
        // ColumnParent(columnFamily),rowKeys ,
        // Selector.newColumnsPredicate(superColumnNames), consistencyLevel);
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                consistencyLevel);
    }

    /** Query related methods */

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
    @Override
    public List executeQuery(String cqlQuery, Class clazz, List<String> relationalField)
    {
        return super.executeQuery(cqlQuery, clazz, relationalField, dataHandler);        
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
    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult)
    {
        // ixClause can be 0,1 or more!
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);

        List<Object> entities = null;
        if (ixClause.isEmpty())
        {
            if (m.isCounterColumnType())
            {

                try
                {
                    IThriftPool thrift = Pelops.getDbConnPool(PelopsUtils.generatePoolName(getPersistenceUnit()));
                    // thrift.get
                    IPooledConnection connection = thrift.getConnection();
                    org.apache.cassandra.thrift.Cassandra.Client thriftClient = connection.getAPI();
                    List<KeySlice> ks = thriftClient.get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, selector.newKeyRange("", "", maxResult), consistencyLevel);
                    connection.release();
                    entities = onCounterColumn(m, isRelation, relations, ks);
                }
                catch (InvalidRequestException irex)
                {
                    log.error("Error during executing find, Caused by :" + irex.getMessage());
                    throw new PersistenceException(irex);
                }
                catch (UnavailableException uex)
                {
                    log.error("Error during executing find, Caused by :" + uex.getMessage());
                    throw new PersistenceException(uex);
                }
                catch (TimedOutException tex)
                {
                    log.error("Error during executing find, Caused by :" + tex.getMessage());
                    throw new PersistenceException(tex);
                }
                catch (TException tex)
                {
                    log.error("Error during executing find, Caused by :" + tex.getMessage());
                    throw new PersistenceException(tex);
                }
            }
            else
            {

                Map<Bytes, List<Column>> qResults = selector.getColumnsFromRows(m.getTableName(),
                        selector.newKeyRange("", "", maxResult), slicePredicate, consistencyLevel);

                entities = new ArrayList<Object>(qResults.size());

                computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                // populateData(m, qResults, entities, isRelation, relations,
                // dataHandler);

            }
        }
        else
        {
            entities = new ArrayList<Object>();
            for (IndexClause ix : ixClause)
            {
                Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate,
                        consistencyLevel);
                computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                // // iterate through complete map and
                // populateData(m, qResults, entities, isRelation, relations,
                // dataHandler);
            }
        }
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
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
            int maxResult)
    {
        return (List<EnhanceEntity>) find(conditions, m, true, relationNames, maxResult);
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
    @Override
    public List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations)
            throws Exception
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        List<Object> entities = null;
        List<KeySlice> keys = selector.getKeySlices(new ColumnParent(m.getTableName()), selector.newKeyRange(
                minVal != null ? Bytes.fromByteArray(minVal) : Bytes.fromUTF8(""),
                maxVal != null ? Bytes.fromByteArray(maxVal) : Bytes.fromUTF8(""), 10000), slicePredicate,
                consistencyLevel);

        List results = null;
        if (keys != null)
        {
            results = populateEntitiesFromKeySlices(m, isWrapReq, relations, keys, dataHandler);
        }

        return results;
    }

    public List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Queue<FilterClause> filterClauseQueue)
    {
        return invertedIndexHandler.search(m, filterClauseQueue, getPersistenceUnit(), consistencyLevel);
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
        if (cLevel != null)
        {
            this.consistencyLevel = cLevel;
        }
        else
        {
            log.warn("Please provide resonable consistency Level");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.CassandraClientBase#getDataHandler()
     */
    @Override
    protected CassandraDataHandler getDataHandler()
    {
        return dataHandler;
    }

}