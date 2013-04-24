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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
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
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient extends CassandraClientBase implements Client<CassQuery>, Batcher, TableGenerator
{
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

    private List<Mutation> mutations;

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
    public PelopsClient(IndexManager indexManager, EntityReader reader, String persistenceUnit,
            Map<String, Object> externalProperties)
    {
        super(persistenceUnit, externalProperties);
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.dataHandler = new PelopsDataHandler(externalProperties, isCQLEnabled);
        this.invertedIndexHandler = new PelopsInvertedIndexHandler(externalProperties);
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
            entities = dataHandler.fromThriftRow(entityClass, metadata, relationNames, isWrapReq,
                    getConsistencyLevel(), rowIds);
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

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        if (isCQL3Enabled(metadata, metaModel))
        {
            String deleteQuery = onDeleteQuery(metadata, metaModel, pKey);
            executeQuery(deleteQuery, metadata.getEntityClazz(), null);
        }
        else
        {

            if (metadata.isCounterColumnType())
            {
                deleteRecordFromCounterColumnFamily(pKey, metadata, getConsistencyLevel());
            }
            else
            {
                RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit(),
                        getExternalProperties()));
                rowDeletor.deleteRow(metadata.getTableName(),
                        CassandraUtilities.toBytes(pKey, metadata.getIdAttribute().getJavaType()),
                        getConsistencyLevel());

            }
        }
        // Delete from Lucene if applicable
        getIndexManager().remove(metadata, entity, pKey.toString());

        // Delete from Inverted Index if applicable
        invertedIndexHandler.delete(entity, metadata, getConsistencyLevel());

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

        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit(), getExternalProperties());
        Mutator mutator = Pelops.createMutator(poolName);

        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(joinTableData.getEntityClass());

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);

            List<Column> columns = new ArrayList<Column>();

            Class columnType = null;
            for (Object value : values)
            {
                Column column = new Column();
                column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName
                        + Constants.JOIN_COLUMN_NAME_SEPARATOR + value.toString()));
                // column.setValue(PropertyAccessorFactory.STRING.toBytes((String)
                // value));
                column.setValue(PropertyAccessorHelper.getBytes(value));
                column.setTimestamp(System.currentTimeMillis());
                columnType = value.getClass();
                columns.add(column);
            }

            createIndexesOnColumns(entityMetadata, joinTableName, poolName, columns, columnType);
            // Object pk = key;

            mutator.writeColumns(joinTableName, Bytes.fromByteArray(PropertyAccessorHelper.getBytes(key)),
                    Arrays.asList(columns.toArray(new Column[0])));
        }
        mutator.execute(getConsistencyLevel());
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Class columnJavaType)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));

        List<Column> columns = selector.getColumnsFromRow(joinTableName,
                Bytes.fromByteArray(PropertyAccessorHelper.getBytes(parentId)),
                Selector.newColumnsPredicateAll(true, 10), getConsistencyLevel());

        List<Object> foreignKeys = dataHandler.getForeignKeysFromJoinTable(inverseJoinColumnName, columns,
                columnJavaType);
        return (List<E>) foreignKeys;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));
        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        // String childIdStr = (String) columnValue;

        IndexClause ix = Selector.newIndexClause(Bytes.EMPTY, 10000, Selector.newIndexExpression(columnName
                + Constants.JOIN_COLUMN_NAME_SEPARATOR + columnValue, IndexOperator.EQ,
                Bytes.fromByteArray(PropertyAccessorHelper.getBytes(columnValue))));

        Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(tableName, ix, slicePredicate,
                getConsistencyLevel());

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

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));
        // rowDeletor.deleteRow(tableName, columnValue.toString(),
        // getConsistencyLevel());
        rowDeletor.deleteRow(tableName, CassandraUtilities.toBytes(columnValue, columnValue.getClass()),
                getConsistencyLevel());

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
        List<Object> entities = null;
        if (isCQLEnabled)
        {
            entities = findByRelationQuery(m, colName, colValue, clazz, dataHandler);
        }
        else
        {

            Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                    getExternalProperties()));

            SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
            IndexClause ix = Selector.newIndexClause(
                    Bytes.EMPTY,
                    10000,
                    Selector.newIndexExpression(colName, IndexOperator.EQ,
                            Bytes.fromByteArray(PropertyAccessorHelper.getBytes(colValue))));
            Map<Bytes, List<Column>> qResults;
            try
            {
                qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate, getConsistencyLevel());
            }
            catch (PelopsException e)
            {
                log.info(e.getMessage());
                return entities;
            }
            entities = new ArrayList<Object>(qResults.size());
            // iterate through complete map and
            populateData(m, qResults, entities, false, null, dataHandler);
        }
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
            throw new UnsupportedOperationException("Merge is not permitted on counter column! ");
        }

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        if (isCQL3Enabled(metadata, metaModel))
        {
            Cassandra.Client client = getRawClient(metadata.getPersistenceUnit(), metadata.getSchema());
            try
            {
                client.set_cql_version(getCqlVersion());
                client.set_keyspace(metadata.getSchema());
                String insert_Query = createInsertQuery(metadata, entity, client, rlHolders);
                client.set_cql_version(getCqlVersion());
                client.execute_cql3_query(ByteBuffer.wrap(insert_Query.getBytes(Constants.CHARSET_UTF8)),
                        Compression.NONE, getConsistencyLevel());
            }
            catch (InvalidRequestException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
            catch (TException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
            catch (UnsupportedEncodingException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
            catch (UnavailableException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
            catch (TimedOutException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
            catch (SchemaDisagreementException e)
            {
                log.error("Error during persist, Caused by:", e);
                throw new KunderaException(e);
            }
        }
        else
        {
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

            Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit(),
                    getExternalProperties()));
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

            mutator.execute(getConsistencyLevel());
            tf = null;
        }
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
        invertedIndexHandler.write(node, entityMetadata, getPersistenceUnit(), getConsistencyLevel(), dataHandler);
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
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));
        List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>();
        rowKeys.add(ByteBuffer.wrap(rowId.getBytes()));

        // Pelops.getDbConnPool("").getConnection().getAPI().

        // selector.getColumnOrSuperColumnsFromRows(new
        // ColumnParent(columnFamily),rowKeys ,
        // Selector.newColumnsPredicate(superColumnNames),
        // getConsistencyLevel());
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                getConsistencyLevel());
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
            int maxResult, List<String> columns)
    {
        // ixClause can be 0,1 or more!
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        if (columns != null && !columns.isEmpty())
        {
            slicePredicate = Selector.newColumnsPredicate(columns.toArray(new String[] {}));
        }

        List<Object> entities = new ArrayList<Object>();
        if (ixClause.isEmpty())
        {
            if (m.isCounterColumnType())
            {

                try
                {
                    IThriftPool thrift = Pelops.getDbConnPool(PelopsUtils.generatePoolName(getPersistenceUnit(),
                            getExternalProperties()));
                    // thrift.get
                    IPooledConnection connection = thrift.getConnection();
                    org.apache.cassandra.thrift.Cassandra.Client thriftClient = connection.getAPI();
                    List<KeySlice> ks = thriftClient.get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, Selector.newKeyRange("", "", maxResult), getConsistencyLevel());
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

                if (m.getType().isSuperColumnFamilyMetadata())
                {
                    Map<Bytes, List<SuperColumn>> qResults = selector.getSuperColumnsFromRows(m.getTableName(),
                            selector.newKeyRange("", "", maxResult), slicePredicate, getConsistencyLevel());
                    entities = new ArrayList<Object>(qResults.size());
                    computeEntityViaSuperColumns(m, isRelation, relations, entities, qResults);
                }
                else
                {
                    Map<Bytes, List<Column>> qResults = selector.getColumnsFromRows(m.getTableName(),
                            selector.newKeyRange("", "", maxResult), slicePredicate, getConsistencyLevel());
                    entities = new ArrayList<Object>(qResults.size());
                    // populateData(m, qResults, entities, isRelation,
                    // relations, dataHandler);
                    computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                }

            }
        }
        else
        {
            entities = new ArrayList<Object>();
            for (IndexClause ix : ixClause)
            {
                Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate,
                        getConsistencyLevel());
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
            int maxResult, List<String> columns)
    {
        return (List<EnhanceEntity>) find(conditions, m, true, relationNames, maxResult, columns);
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
    public List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations,
            List<String> columns, List<IndexExpression> conditions) throws Exception
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit(),
                getExternalProperties()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        if (columns != null && !columns.isEmpty())
        {
            slicePredicate = Selector.newColumnsPredicate(columns.toArray(new String[] {}));
        }

        KeyRange keyRange = selector.newKeyRange(minVal != null ? Bytes.fromByteArray(minVal) : Bytes.fromUTF8(""),
                maxVal != null ? Bytes.fromByteArray(maxVal) : Bytes.fromUTF8(""), 10000);
        if (conditions != null)
        {
            keyRange.setRow_filter(conditions);
            keyRange.setRow_filterIsSet(true);
        }
        // List<Object> entities = null;
        List<KeySlice> keys = selector.getKeySlices(new ColumnParent(m.getTableName()), keyRange, slicePredicate,
                getConsistencyLevel());

        List results = null;
        if (keys != null)
        {
            results = populateEntitiesFromKeySlices(m, isWrapReq, relations, keys, dataHandler);
        }

        return results;
    }

    public List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Map<Boolean, List<IndexClause>> indexClauseMap)
    {

        return invertedIndexHandler.search(m, getPersistenceUnit(), getConsistencyLevel(), indexClauseMap);
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

    protected IPooledConnection getPooledConection(String persistenceUnit)
    {
        return PelopsUtils.getCassandraConnection(persistenceUnit, getExternalProperties());

    }

    protected void releaseConnection(Object conn)
    {
        PelopsUtils.releaseConnection((IPooledConnection) conn);
    }

    @Override
    public Long generate(TableGeneratorDiscriptor discriptor)
    {
        return getGeneratedValue(discriptor, getPersistenceUnit());
    }
}
