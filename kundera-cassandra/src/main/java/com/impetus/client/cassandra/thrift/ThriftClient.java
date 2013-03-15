/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.cassandra.thrift;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EmbeddableType;

import net.dataforte.cassandra.pool.ConnectionPool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
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
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Kundera Client implementation for Cassandra using Thrift library
 * 
 * @author amresh.singh
 */
public class ThriftClient extends CassandraClientBase implements Client, Batcher, TableGenerator
{

    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftClient.class);

    /** The data handler. */
    private ThriftDataHandler dataHandler;

    /** Handler for Inverted indexing */
    private InvertedIndexHandler invertedIndexHandler;

    /** The reader. */
    private EntityReader reader;

    private ConnectionPool pool;

    /** The timestamp. */
    private long timestamp;

    public ThriftClient(IndexManager indexManager, EntityReader reader, String persistenceUnit, ConnectionPool pool,
            Map<String, Object> externalProperties)
    {
        super(persistenceUnit, externalProperties);
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.dataHandler = new ThriftDataHandler(pool);
        this.invertedIndexHandler = new ThriftInvertedIndexHandler(pool);
        this.reader = reader;
        this.pool = pool;
    }   

    /**
     * Persists a {@link Node} to database
     */
    @Override
    public void persist(Object entity, Object id, List<RelationHolder> rlHolders)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        
        Cassandra.Client conn = PelopsUtils.getCassandraConnection(pool);
        try
        {
            Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            // if entity is embeddable...call cql translator to get cql string!
            // use thrift client to execute cql query.
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                onpersistOverCompositeKey(entityMetadata, entity, conn, rlHolders);
            }
            else
            {
                prepareMutation(entityMetadata, entity, id, rlHolders, mutationMap);
                // Write Mutation map to database
                // conn.set_cql_version("3.0.0");
                conn.batch_mutate(mutationMap, getConsistencyLevel());
            }
            mutationMap.clear();
            mutationMap = null;
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        catch (SchemaDisagreementException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Error while persisting record. Details: ", e);
            throw new KunderaException(e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

    }

    /**
     * Persists a Join table record set into database
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit(), externalProperties);

        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(joinTableData.getEntityClass());
        Cassandra.Client conn = null;
        try
        {

            for (Object key : joinTableRecords.keySet())
            {
                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor((Field) entityMetadata
                        .getIdAttribute().getJavaMember());
                byte[] rowKey = accessor.toBytes(key);

                Set<Object> values = joinTableRecords.get(key);
                List<Column> columns = new ArrayList<Column>();

                // Create Insertion List
                List<Mutation> insertionList = new ArrayList<Mutation>();

                for (Object value : values)
                {
                    Column column = new Column();
                    column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName
                            + Constants.JOIN_COLUMN_NAME_SEPARATOR + (String) value));
                    column.setValue(PropertyAccessorFactory.STRING.toBytes((String) value));
                    column.setTimestamp(System.currentTimeMillis());

                    columns.add(column);

                    Mutation mut = new Mutation();
                    mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                    insertionList.add(mut);
                }

                createIndexesOnColumns(entityMetadata.getSchema(), joinTableName, poolName, columns);

                // Create Mutation Map
                Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                columnFamilyValues.put(joinTableName, insertionList);
                Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

                // Write Mutation map to database

                conn = PelopsUtils.getCassandraConnection(pool);

                conn.set_keyspace(entityMetadata.getSchema());

                conn.batch_mutate(mulationMap, getConsistencyLevel());

            }
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while inserting record into join table. Details: " + e.getMessage());
            throw new PersistenceException("Error while inserting record into join table", e);
        }
        catch (TException e)
        {
            log.error("Error while inserting record into join table. Details: " + e.getMessage());
            throw new PersistenceException("Error while inserting record into join table", e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while inserting record into join table. Details: " + e.getMessage());
            throw new PersistenceException("Error while inserting record into join table", e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while inserting record into join table. Details: " + e.getMessage());
            throw new PersistenceException("Error while inserting record into join table", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }
    }

    /**
     * Indexes a {@link Node} to database
     */
    @Override
    protected void indexNode(Node node, EntityMetadata entityMetadata)
    {
        super.indexNode(node, entityMetadata);

        // Write to inverted index table if applicable
        // setCassandraClient();
        invertedIndexHandler.write(node, entityMetadata, getPersistenceUnit(), getConsistencyLevel(), dataHandler);

    }

    /**
     * Finds an entity from database
     */
    @Override
    public Object find(Class entityClass, Object key)
    {
        return super.find(entityClass, key);
    }

    /**
     * Finds a {@link List} of entities from database
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        return super.findAll(entityClass, keys);
    }

    /**
     * Finds a {@link List} of entities from database
     */
    @Override
    public final List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds)
    {
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
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

    /**
     * Finds a {@link List} of entities from database for given super columns
     */
    @Override
    public List find(Class<?> entityClass, Map<String, String> embeddedColumnMap)
    {
        return super.find(entityClass, embeddedColumnMap, dataHandler);
    }

    /**
     * Loads super columns from database
     */
    @Override
    protected final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames)
    {
        if (!isOpen())
            throw new PersistenceException("ThriftClient is closed.");

        byte[] rowKey = rowId.getBytes();

        SlicePredicate predicate = new SlicePredicate();
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (String superColumnName : superColumnNames)
        {
            columnNames.add(ByteBuffer.wrap(superColumnName.getBytes()));
        }

        predicate.setColumn_names(columnNames);

        ColumnParent parent = new ColumnParent(columnFamily);
        List<ColumnOrSuperColumn> coscList;
        Cassandra.Client conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(pool);

            coscList = conn.get_slice(ByteBuffer.wrap(rowKey), parent, predicate, getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while getting super columns for row Key " + rowId + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting super columns for row Key " + rowId, e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while getting columns for row Key " + rowId + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting super columns for row Key " + rowId, e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while getting columns for row Key " + rowId + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting super columns for row Key " + rowId, e);
        }
        catch (TException e)
        {
            log.error("Error while getting columns for row Key " + rowId + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting super columns for row Key " + rowId, e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        List<SuperColumn> superColumns = ThriftDataResultHelper.transformThriftResult(coscList,
                ColumnFamilyType.SUPER_COLUMN, null);
        return superColumns;
    }

    /**
     * Retrieves column for a given primary key
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {

        byte[] rowKey = pKeyColumnValue.toString().getBytes();

        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        ColumnParent parent = new ColumnParent(tableName);
        List<ColumnOrSuperColumn> results;
        Cassandra.Client conn = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);

            conn = PelopsUtils.getCassandraConnection(pool);
            results = conn.get_slice(ByteBuffer.wrap(rowKey), parent, predicate, getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while getting columns for row Key " + pKeyColumnValue + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting columns for row Key " + pKeyColumnValue, e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while getting columns for row Key " + pKeyColumnValue + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting columns for row Key " + pKeyColumnValue, e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while getting columns for row Key " + pKeyColumnValue + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting columns for row Key " + pKeyColumnValue, e);
        }
        catch (TException e)
        {
            log.error("Error while getting columns for row Key " + pKeyColumnValue + ". Details:" + e.getMessage());
            throw new EntityReaderException("Error while getting columns for row Key " + pKeyColumnValue, e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        List<Column> columns = ThriftDataResultHelper.transformThriftResult(results, ColumnFamilyType.COLUMN, null);

        List<E> foreignKeys = dataHandler.getForeignKeysFromJoinTable(columnName, columns);
        return foreignKeys;
    }

    /**
     * Retrieves IDs for a given column
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        SlicePredicate slicePredicate = new SlicePredicate();

        slicePredicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), false, 1000));

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        String childIdStr = (String) columnValue;
        IndexExpression ie = new IndexExpression(Bytes.fromUTF8(
                columnName + Constants.JOIN_COLUMN_NAME_SEPARATOR + childIdStr).getBytes(), IndexOperator.EQ, Bytes
                .fromUTF8(childIdStr).getBytes());

        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        expressions.add(ie);

        IndexClause ix = new IndexClause();
        ix.setStart_key(Bytes.EMPTY.toByteArray());
        ix.setCount(1000);
        ix.setExpressions(expressions);

        List<Object> rowKeys = new ArrayList<Object>();
        ColumnParent columnParent = new ColumnParent(tableName);
        Cassandra.Client conn = null;
        try
        {
            String keyspace = CassandraUtilities.getKeyspace(persistenceUnit);

            conn = PelopsUtils.getCassandraConnection(pool);
            List<KeySlice> keySlices = conn.get_indexed_slices(columnParent, ix, slicePredicate, getConsistencyLevel());

            rowKeys = ThriftDataResultHelper.getRowKeys(keySlices, metadata);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while fetching key slices for index clause. Details:" + e.getMessage());
            throw new KunderaException("Error while fetching key slices for index clause", e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while fetching key slices for index clause. Details:" + e.getMessage());
            throw new KunderaException("Error while fetching key slices for index clause", e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while fetching key slices for index clause. Details:" + e.getMessage());
            throw new KunderaException("Error while fetching key slices for index clause", e);
        }
        catch (TException e)
        {
            log.error("Error while fetching key slices for index clause. Details:" + e.getMessage());
            throw new KunderaException("Error while fetching key slices for index clause", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }

        return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);

        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), false, 1000));
        List<Object> entities = null;

        IndexExpression ie = new IndexExpression(Bytes.fromUTF8(colName).getBytes(), IndexOperator.EQ,
                ByteBuffer.wrap(PropertyAccessorHelper.getBytes(colValue)));
        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        expressions.add(ie);

        IndexClause ix = new IndexClause();
        ix.setStart_key(Bytes.EMPTY.toByteArray());
        ix.setCount(1000);
        ix.setExpressions(expressions);
        ColumnParent columnParent = new ColumnParent(m.getTableName());

        List<KeySlice> keySlices;
        Cassandra.Client conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(pool);
            keySlices = conn.get_indexed_slices(columnParent, ix, slicePredicate, getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            if (e.why != null && e.why.contains("No indexed columns"))
            {
                return entities;
            }
            else
            {
                log.error("Error while finding relations. Details:" + e.getMessage());
                throw new KunderaException("Error while finding relations", e);
            }

        }
        catch (UnavailableException e)
        {
            log.error("Error while finding relations. Details:" + e.getMessage());
            throw new KunderaException("Error while finding relations", e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while finding relations. Details:" + e.getMessage());
            throw new KunderaException("Error while finding relations", e);
        }
        catch (TException e)
        {
            log.error("Error while finding relations. Details:" + e.getMessage());
            throw new KunderaException("Error while finding relations", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        if (keySlices != null)
        {
            entities = new ArrayList<Object>(keySlices.size());
            populateData(m, keySlices, entities, false, null);
        }

        return entities;
    }

    @Override
    public void delete(Object entity, Object pKey, List<RelationHolder> rlHolders)
    {
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        Cassandra.Client conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(pool);

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
            {
                EmbeddableType compoundKey = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
                onDeleteQuery(metadata, metaModel, pKey, compoundKey);
            }
            else
            {

                if (metadata.isCounterColumnType())
                {
                    deleteRecordFromCounterColumnFamily(pKey, metadata, getConsistencyLevel());
                }
                else
                {
                    ColumnPath path = new ColumnPath(metadata.getTableName());

                    conn.remove(ByteBuffer.wrap(CassandraUtilities.toBytes(pKey,
                            metadata.getIdAttribute().getJavaType()).toByteArray()), path, System.currentTimeMillis(),
                            getConsistencyLevel());
                }
            }
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while deleting row from table. Details:" + e.getMessage());
            throw new KunderaException("Error while deleting row from table", e);
        }
        catch (TException e)
        {
            log.error("Error while deleting row from table. Details:" + e.getMessage());
            throw new KunderaException("Error while deleting row from table", e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while deleting row from table. Details:" + e.getMessage());
            throw new KunderaException("Error while deleting row from table", e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while deleting row from table. Details:" + e.getMessage());
            throw new KunderaException("Error while deleting row from table", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        // Delete from Lucene if applicable
        getIndexManager().remove(metadata, entity, pKey.toString());

        // Delete from Inverted Index if applicable
        invertedIndexHandler.delete(entity, metadata, getConsistencyLevel());
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }

        Cassandra.Client conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(pool);

            ColumnPath path = new ColumnPath(tableName);
            // cassandra_client.remove(ByteBuffer.wrap(columnValue.toString().getBytes()),
            // path,
            // System.currentTimeMillis(), getConsistencyLevel());
            conn.remove(ByteBuffer.wrap(CassandraUtilities.toBytes(columnValue, columnValue.getClass()).toByteArray()),
                    path, System.currentTimeMillis(), getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while deleting column value. Details:" + e.getMessage());
            throw new PersistenceException("Error while deleting column value", e);
        }
        catch (TException e)
        {
            log.error("Error while deleting column value. Details:" + e.getMessage());
            throw new PersistenceException("Error while deleting column value", e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while deleting column value. Details:" + e.getMessage());
            throw new PersistenceException("Error while deleting column value", e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while deleting column value. Details:" + e.getMessage());
            throw new PersistenceException("Error while deleting column value", e);
        }
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

    }
 
    @Override
    public Class<?> getDefaultQueryImplementor()
    {
        return CassQuery.class;
    }

    @Override
    public String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    @Override
    protected List<RelationHolder> getRelationHolders(Node node)
    {
        return super.getRelationHolders(node);
    }

    @Override
    public void close()
    {
        this.indexManager.flush();
        this.dataHandler = null;
        this.invertedIndexHandler = null;
        super.close();

    }

    private void populateData(EntityMetadata m, List<KeySlice> keySlices, List<Object> entities, boolean isRelational,
            List<String> relationNames)
    {
        try
        {
            if (m.getType().isSuperColumnFamilyMetadata())
            {
                List<Object> rowKeys = ThriftDataResultHelper.getRowKeys(keySlices, m);

                Object[] rowIds = rowKeys.toArray();
                entities.addAll(findAll(m.getEntityClazz(), rowIds));
            }
            else
            {
                for (KeySlice keySlice : keySlices)
                {
                    byte[] key = keySlice.getKey();
                    List<ColumnOrSuperColumn> coscList = keySlice.getColumns();

                    List<Column> columns = ThriftDataResultHelper.transformThriftResult(coscList,
                            ColumnFamilyType.COLUMN, null);

                    Object e = dataHandler.populateEntity(
                            new ThriftRow(PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key), m
                                    .getTableName(), columns, new ArrayList<SuperColumn>(0),
                                    new ArrayList<CounterColumn>(0), new ArrayList<CounterSuperColumn>(0)), m,
                            relationNames, isRelational);
                    if (e != null)
                    {
                        entities.add(e);
                    }
                }
            }
        }
        catch (Exception e)
        {
            log.error("Error while populating data for relations. Details: " + e.getMessage());
            throw new KunderaException("Error while populating data for relations", e);
        }

    }

    /** Query related methods */

    @Override
    public List executeQuery(String cqlQuery, Class clazz, List<String> relationalField)
    {
        return super.executeQuery(cqlQuery, clazz, relationalField, dataHandler);
    }

    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult, List<String> columns)
    {
        List<Object> entities = null;
        Cassandra.Client conn = null;
        try
        {
            // ixClause can be 0,1 or more!
            SlicePredicate slicePredicate = new SlicePredicate();

            if (columns != null && !columns.isEmpty())
            {
                List asList = new ArrayList(32);
                for (String colName : columns)
                {
                    if (colName != null)
                    {
                        asList.add(Bytes.fromUTF8(colName).getBytes());
                    }
                }
                slicePredicate.setColumn_names(asList);
            }
            else
            {
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(Bytes.EMPTY.getBytes());
                sliceRange.setFinish(Bytes.EMPTY.getBytes());
                slicePredicate.setSlice_range(sliceRange);
            }
            conn = PelopsUtils.getCassandraConnection(pool);

            if (ixClause.isEmpty())
            {
                KeyRange keyRange = new KeyRange(maxResult);
                keyRange.setStart_key(Bytes.nullSafeGet(Bytes.fromUTF8("")));
                keyRange.setEnd_key(Bytes.nullSafeGet(Bytes.fromUTF8("")));

                if (m.isCounterColumnType())
                {

                    List<KeySlice> ks = conn.get_range_slices(new ColumnParent(m.getTableName()), slicePredicate,
                            keyRange, getConsistencyLevel());
                    entities = onCounterColumn(m, isRelation, relations, ks);

                }
                else
                {

                    List<KeySlice> keySlices = conn.get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, keyRange, getConsistencyLevel());

                    if (m.getType().isSuperColumnFamilyMetadata())
                    {
                        Map<Bytes, List<SuperColumn>> qResults = ThriftDataResultHelper.transformThriftResult(
                                ColumnFamilyType.SUPER_COLUMN, keySlices, null);
                        entities = new ArrayList<Object>(qResults.size());
                        computeEntityViaSuperColumns(m, isRelation, relations, entities, qResults);
                    }
                    else
                    {
                        Map<Bytes, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
                                ColumnFamilyType.COLUMN, keySlices, null);
                        entities = new ArrayList<Object>(qResults.size());
                        computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                    }
                }
            }
            else
            {
                entities = new ArrayList<Object>();
                for (IndexClause ix : ixClause)
                {
                    List<KeySlice> keySlices = conn.get_indexed_slices(new ColumnParent(m.getTableName()), ix,
                            slicePredicate, getConsistencyLevel());

                    Map<Bytes, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
                            ColumnFamilyType.COLUMN, keySlices, null);
                    // iterate through complete map and
                    entities = new ArrayList<Object>(qResults.size());

                    computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                }
            }
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
        finally
        {
            PelopsUtils.releaseConnection(pool, conn);
        }

        return entities;
    }

    @Override
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
            int maxResult, List<String> columns)
    {
        return (List<EnhanceEntity>) find(conditions, m, true, relationNames, maxResult, columns);
    }

    @Override
    public List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations,
            List<String> columns, List<IndexExpression> conditions) throws Exception
    {
        SlicePredicate slicePredicate = new SlicePredicate();

        if (columns != null && !columns.isEmpty())
        {
            List asList = new ArrayList(32);
            for (String colName : columns)
            {
                if (colName != null)
                {
                    asList.add(Bytes.fromUTF8(colName).getBytes());
                }
            }
            slicePredicate.setColumn_names(asList);
        }
        else
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(Bytes.EMPTY.getBytes());
            sliceRange.setFinish(Bytes.EMPTY.getBytes());
            slicePredicate.setSlice_range(sliceRange);
        }

        KeyRange keyRange = new KeyRange(10000);
        keyRange.setStart_key(minVal == null ? "".getBytes() : minVal);
        keyRange.setEnd_key(maxVal == null ? "".getBytes() : maxVal);
        ColumnParent cp = new ColumnParent(m.getTableName());

        if (conditions != null && !conditions.isEmpty())
        {
            keyRange.setRow_filter(conditions);
            keyRange.setRow_filterIsSet(true);
        }

        Cassandra.Client conn = PelopsUtils.getCassandraConnection(pool);

        List<KeySlice> keys = conn.get_range_slices(cp, slicePredicate, keyRange, getConsistencyLevel());

        PelopsUtils.releaseConnection(pool, conn);

        List results = null;
        if (keys != null)
        {
            results = populateEntitiesFromKeySlices(m, isWrapReq, relations, keys, dataHandler);
        }

        return results;
    }

    @Override
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

    protected Cassandra.Client getPooledConection(String persistenceUnit)
    {
        return PelopsUtils.getCassandraConnection(this.pool);
    }

    protected void releaseConnection(Object conn)
    {
        PelopsUtils.releaseConnection(this.pool, (Cassandra.Client) conn);
    }

    @Override
    public Long generate(TableGeneratorDiscriptor discriptor)
    {
        return getGeneratedValue(discriptor,getPersistenceUnit());
    }
}
