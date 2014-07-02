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
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EntityType;

import net.dataforte.cassandra.pool.ConnectionPool;

import org.apache.cassandra.db.marshal.UTF8Type;
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
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.client.cassandra.thrift.ThriftClientFactory.Connection;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.TimestampGenerator;

/**
 * Kundera Client implementation for Cassandra using Thrift library
 * 
 * @author amresh.singh
 */
public class ThriftClient extends CassandraClientBase implements Client<CassQuery>, Batcher, TableGenerator,
        AutoGenerator
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(ThriftClient.class);

    /** The data handler. */
    private final ThriftDataHandler dataHandler;

    /** Handler for Inverted indexing */
    private final InvertedIndexHandler invertedIndexHandler;

    /** The reader. */
    private final EntityReader reader;

    private final ThriftClientFactory clientFactory;

    private final ConnectionPool pool;

    public ThriftClient(ThriftClientFactory clientFactory, IndexManager indexManager, EntityReader reader,
            String persistenceUnit, ConnectionPool pool, Map<String, Object> externalProperties,
            final KunderaMetadata kunderaMetadata, final TimestampGenerator generator)
    {
        super(persistenceUnit, externalProperties, kunderaMetadata, generator);
        this.clientFactory = clientFactory;
        this.indexManager = indexManager;
        this.dataHandler = new ThriftDataHandler(this, kunderaMetadata, generator);
        this.reader = reader;
        this.clientMetadata = clientFactory.getClientMetadata();
        this.invertedIndexHandler = new ThriftInvertedIndexHandler(this,
                MetadataUtils.useSecondryIndex(clientMetadata), generator);
        this.pool = pool;
    }

    /**
     * Persists and indexes a {@link Node} to database
     */
    @Override
    public void persist(Node node)
    {
        super.persist(node);
    }

    /**
     * Persists a {@link Node} to database
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        Connection conn = getConnection();
        try
        {
            // if entity is embeddable...call cql translator to get cql string!
            // use thrift client to execute cql query.

            if (isCql3Enabled(entityMetadata))
            {
                cqlClient.persist(entityMetadata, entity, conn.getClient(), rlHolders,
                        getTtlValues().get(entityMetadata.getTableName()));
            }
            else
            {
                Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                prepareMutation(entityMetadata, entity, id, rlHolders, mutationMap);
                // Write Mutation map to database
                conn.getClient().batch_mutate(mutationMap, getConsistencyLevel());

                mutationMap.clear();
                mutationMap = null;
            }

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Error while persisting record, Caused by: .", e);
            throw new KunderaException(e);
        }
        finally
        {
            releaseConnection(conn);

            if (isTtlPerRequest())
            {
                getTtlValues().clear();
            }
        }
    }

    /**
     * Persists a Join table record set into database
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                joinTableData.getEntityClass());

        Connection conn = null;
        try
        {

            conn = getConnection();

            if (isCql3Enabled(entityMetadata))
            {
                persistJoinTableByCql(joinTableData, conn.getClient());
            }
            else
            {
                KunderaCoreUtils.showQuery("Persist join table:" + joinTableName, showQuery);
                for (Object key : joinTableRecords.keySet())
                {
                    PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor((Field) entityMetadata
                            .getIdAttribute().getJavaMember());
                    byte[] rowKey = accessor.toBytes(key);

                    Set<Object> values = joinTableRecords.get(key);
                    List<Column> columns = new ArrayList<Column>();

                    // Create Insertion List
                    List<Mutation> insertionList = new ArrayList<Mutation>();
                    Class columnType = null;
                    for (Object value : values)
                    {
                        Column column = new Column();
                        column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName
                                + Constants.JOIN_COLUMN_NAME_SEPARATOR + value));
                        column.setValue(PropertyAccessorHelper.getBytes(value));

                        column.setTimestamp(generator.getTimestamp());
                        columnType = value.getClass();
                        columns.add(column);

                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                        insertionList.add(mut);
                    }
                    createIndexesOnColumns(entityMetadata, joinTableName, columns, columnType);
                    // Create Mutation Map
                    Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                    columnFamilyValues.put(joinTableName, insertionList);
                    Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                    mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

                    // Write Mutation map to database

                    conn.getClient().set_keyspace(entityMetadata.getSchema());

                    conn.getClient().batch_mutate(mulationMap, getConsistencyLevel());
                }
            }
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while inserting record into join table, Caused by: .", e);
            throw new PersistenceException(e);
        }
        catch (TException e)
        {
            log.error("Error while inserting record into join table, Caused by: .", e);
            throw new PersistenceException(e);
        }
        finally
        {
            releaseConnection(conn);
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
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        return super.findAll(entityClass, columnsToSelect, keys);
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
        return findByRowKeys(entityClass, relationNames, isWrapReq, metadata, rowIds);
    }

    /**
     * Finds a {@link List} of entities from database for given super columns
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
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
        // TODO::: super column abstract entity and discriminator column
        if (!isOpen())
            throw new PersistenceException("ThriftClient is closed.");

        byte[] rowKey = rowId.getBytes();

        SlicePredicate predicate = new SlicePredicate();
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (String superColumnName : superColumnNames)
        {
            KunderaCoreUtils.showQuery("Fetch superColumn:" + superColumnName, showQuery);
            columnNames.add(ByteBuffer.wrap(superColumnName.getBytes()));
        }

        predicate.setColumn_names(columnNames);

        ColumnParent parent = new ColumnParent(columnFamily);
        List<ColumnOrSuperColumn> coscList;
        Connection conn = null;
        try
        {
            conn = getConnection();

            coscList = conn.getClient().get_slice(ByteBuffer.wrap(rowKey), parent, predicate, getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while getting super columns for row Key {} , Caused by: .", rowId, e);
            throw new EntityReaderException(e);
        }
        catch (UnavailableException e)
        {
            log.error("Error while getting super columns for row Key {} , Caused by: .", rowId, e);
            throw new EntityReaderException(e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while getting super columns for row Key {} , Caused by: .", rowId, e);
            throw new EntityReaderException(e);
        }
        catch (TException e)
        {
            log.error("Error while getting super columns for row Key {} , Caused by: .", rowId, e);
            throw new EntityReaderException(e);
        }
        finally
        {

            releaseConnection(conn);
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
            Object pKeyColumnValue, Class columnJavaType)
    {
        List<Object> foreignKeys = new ArrayList<Object>();

        if (getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0))
        {
            foreignKeys = getColumnsByIdUsingCql(schemaName, tableName, pKeyColumnName, columnName, pKeyColumnValue,
                    columnJavaType);
        }
        else
        {
            byte[] rowKey = CassandraUtilities.toBytes(pKeyColumnValue);

            if (rowKey != null)
            {
                SlicePredicate predicate = new SlicePredicate();
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(new byte[0]);
                sliceRange.setFinish(new byte[0]);
                predicate.setSlice_range(sliceRange);

                ColumnParent parent = new ColumnParent(tableName);
                List<ColumnOrSuperColumn> results;
                Connection conn = null;
                try
                {
                    conn = getConnection();
                    results = conn.getClient().get_slice(ByteBuffer.wrap(rowKey), parent, predicate,
                            getConsistencyLevel());
                }
                catch (InvalidRequestException e)
                {
                    log.error("Error while getting columns for row Key {} , Caused by: .", pKeyColumnValue, e);
                    throw new EntityReaderException(e);
                }
                catch (UnavailableException e)
                {
                    log.error("Error while getting columns for row Key {} , Caused by: .", pKeyColumnValue, e);
                    throw new EntityReaderException(e);
                }
                catch (TimedOutException e)
                {
                    log.error("Error while getting columns for row Key {} , Caused by: .", pKeyColumnValue, e);
                    throw new EntityReaderException(e);
                }
                catch (TException e)
                {
                    log.error("Error while getting columns for row Key {} , Caused by: .", pKeyColumnValue, e);
                    throw new EntityReaderException(e);
                }
                finally
                {
                    releaseConnection(conn);
                }

                List<Column> columns = ThriftDataResultHelper.transformThriftResult(results, ColumnFamilyType.COLUMN,
                        null);

                foreignKeys = dataHandler.getForeignKeysFromJoinTable(columnName, columns, columnJavaType);
            }
        }
        return (List<E>) foreignKeys;
    }

    /**
     * Retrieves IDs for a given column
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        List<Object> rowKeys = new ArrayList<Object>();

        if (getCqlVersion().equalsIgnoreCase(CassandraConstants.CQL_VERSION_3_0))
        {
            rowKeys = findIdsByColumnUsingCql(schemaName, tableName, pKeyName, columnName, columnValue, entityClazz);
        }
        else
        {
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
            SlicePredicate slicePredicate = new SlicePredicate();

            slicePredicate.setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE));

            String childIdStr = PropertyAccessorHelper.getString(columnValue);
            IndexExpression ie = new IndexExpression(UTF8Type.instance.decompose(columnName
                    + Constants.JOIN_COLUMN_NAME_SEPARATOR + childIdStr), IndexOperator.EQ,
                    UTF8Type.instance.decompose(childIdStr));

            List<IndexExpression> expressions = new ArrayList<IndexExpression>();
            expressions.add(ie);

            IndexClause ix = new IndexClause();
            ix.setStart_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ix.setCount(Integer.MAX_VALUE);
            ix.setExpressions(expressions);

            ColumnParent columnParent = new ColumnParent(tableName);
            Connection conn = null;
            try
            {
                conn = getConnection();
                List<KeySlice> keySlices = conn.getClient().get_indexed_slices(columnParent, ix, slicePredicate,
                        getConsistencyLevel());

                rowKeys = ThriftDataResultHelper.getRowKeys(keySlices, metadata);
            }
            catch (InvalidRequestException e)
            {
                log.error("Error while fetching key slices of column family {} for column name {} , Caused by: .",
                        tableName, columnName, e);
                throw new KunderaException(e);
            }
            catch (UnavailableException e)
            {
                log.error("Error while fetching key slices of column family {} for column name {} , Caused by: .",
                        tableName, columnName, e);
                throw new KunderaException(e);
            }
            catch (TimedOutException e)
            {
                log.error("Error while fetching key slices of column family {} for column name {} , Caused by: .",
                        tableName, columnName, e);
                throw new KunderaException(e);
            }
            catch (TException e)
            {
                log.error("Error while fetching key slices of column family {} for column name {} , Caused by: .",
                        tableName, columnName, e);
                throw new KunderaException(e);
            }
            finally
            {
                releaseConnection(conn);
            }
        }
        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }

        if (log.isInfoEnabled())
        {
            log.info("No record found!, returning null.");
        }
        return null;
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        List<Object> entities = null;

        if (isCql3Enabled(m))
        {
            entities = new ArrayList<Object>();

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            EntityType entityType = metaModel.entity(m.getEntityClazz());

            List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

            if (subManagedType.isEmpty())
            {
                entities.addAll(findByRelationQuery(m, colName, colValue, entityClazz, dataHandler));
            }
            else
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            subEntity.getJavaType());

                    entities.addAll(findByRelationQuery(subEntityMetadata, colName, colValue,
                            subEntityMetadata.getEntityClazz(), dataHandler));
                    // TODOO:: if(entities != null)
                }
            }

        }
        else
        {
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE));

            IndexExpression ie = new IndexExpression(UTF8Type.instance.decompose(colName), IndexOperator.EQ,
                    ByteBuffer.wrap(PropertyAccessorHelper.getBytes(colValue)));
            List<IndexExpression> expressions = new ArrayList<IndexExpression>();
            expressions.add(ie);

            IndexClause ix = new IndexClause();
            ix.setStart_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ix.setCount(Integer.MAX_VALUE);
            ix.setExpressions(expressions);
            ColumnParent columnParent = new ColumnParent(m.getTableName());

            List<KeySlice> keySlices;
            Connection conn = null;
            try
            {
                conn = getConnection();
                keySlices = conn.getClient()
                        .get_indexed_slices(columnParent, ix, slicePredicate, getConsistencyLevel());
            }
            catch (InvalidRequestException e)
            {
                if (e.why != null && e.why.contains("No indexed columns"))
                {
                    return entities;
                }
                else
                {
                    log.error("Error while finding relations for column family {} , Caused by: .", m.getTableName(), e);
                    throw new KunderaException(e);
                }
            }
            catch (UnavailableException e)
            {
                log.error("Error while finding relations for column family {} , Caused by: .", m.getTableName(), e);
                throw new KunderaException(e);
            }
            catch (TimedOutException e)
            {
                log.error("Error while finding relations for column family {} , Caused by: .", m.getTableName(), e);
                throw new KunderaException(e);
            }
            catch (TException e)
            {
                log.error("Error while finding relations for column family {} , Caused by: .", m.getTableName(), e);
                throw new KunderaException(e);
            }
            finally
            {
                releaseConnection(conn);
            }

            if (keySlices != null)
            {
                entities = new ArrayList<Object>(keySlices.size());

                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                        m.getPersistenceUnit());

                EntityType entityType = metaModel.entity(m.getEntityClazz());

                List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

                if (subManagedType.isEmpty())
                {
                    entities = populateData(m, keySlices, entities, m.getRelationNames() != null, m.getRelationNames());
                }
                else
                {
                    for (AbstractManagedType subEntity : subManagedType)
                    {
                        EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                subEntity.getJavaType());
                        entities = populateData(subEntityMetadata, keySlices, entities,
                                subEntityMetadata.getRelationNames() != null, subEntityMetadata.getRelationNames());
                        // TODOO:: if(entities != null)

                    }
                }
            }
        }
        return entities;
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        Connection conn = null;
        try
        {
            conn = getConnection();
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

            AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
            // For secondary tables.
            List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                    .getSecondaryTablesName();
            secondaryTables.add(metadata.getTableName());

            for (String tableName : secondaryTables)
            {
                if (isCql3Enabled(metadata))
                {
                    String deleteQuery = onDeleteQuery(metadata, tableName, metaModel, pKey);
                    executeCQLQuery(deleteQuery, isCql3Enabled(metadata));

                }
                else
                {
                    if (metadata.isCounterColumnType())
                    {
                        deleteRecordFromCounterColumnFamily(pKey, tableName, metadata, getConsistencyLevel());
                    }
                    else
                    {
                        ColumnPath path = new ColumnPath(tableName);

                        conn.getClient().remove(
                                CassandraUtilities.toBytes(pKey, metadata.getIdAttribute().getJavaType()), path,
                                generator.getTimestamp(), getConsistencyLevel());
                    }
                }
            }
            // Delete from Lucene if applicable
            getIndexManager().remove(metadata, entity, pKey.toString());

            // Delete from Inverted Index if applicable
            invertedIndexHandler.delete(entity, metadata, getConsistencyLevel(), kunderaMetadata);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while deleting of column family {} for row key {}, Caused by: .", metadata.getTableName(),
                    pKey, e);
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error("Error while deleting of column family {} for row key {}, Caused by: .", metadata.getTableName(),
                    pKey, e);
            throw new KunderaException(e);
        }
        finally
        {
            releaseConnection(conn);
        }
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }

        Connection conn = null;
        try
        {
            conn = getConnection();
            ColumnPath path = new ColumnPath(tableName);
            conn.getClient().remove(CassandraUtilities.toBytes(columnValue, columnValue.getClass()), path,
                    generator.getTimestamp(), getConsistencyLevel());

        }
        catch (InvalidRequestException e)
        {
            log.error("Error while deleting of column family {} for row key {}, Caused by: .", tableName, columnValue,
                    e);
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error("Error while deleting of column family {} for row key {}, Caused by: .", tableName, columnValue,
                    e);
            throw new KunderaException(e);
        }
        finally
        {
            releaseConnection(conn);
        }

    }

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
//        this.dataHandler = null;
//        this.invertedIndexHandler = null;
        super.close();
    }

    private List populateData(EntityMetadata m, List<KeySlice> keySlices, List<Object> entities, boolean isRelational,
            List<String> relationNames)
    {
        try
        {
            if (m.getType().isSuperColumnFamilyMetadata())
            {
                List<Object> rowKeys = ThriftDataResultHelper.getRowKeys(keySlices, m);

                Object[] rowIds = rowKeys.toArray();
                entities.addAll(findAll(m.getEntityClazz(), null, rowIds));
            }
            else
            {
                for (KeySlice keySlice : keySlices)
                {
                    byte[] key = keySlice.getKey();
                    List<ColumnOrSuperColumn> coscList = keySlice.getColumns();

                    List<Column> columns = ThriftDataResultHelper.transformThriftResult(coscList,
                            ColumnFamilyType.COLUMN, null);
                    Object e = null;
                    Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key);
                    e = dataHandler.populateEntity(new ThriftRow(id, m.getTableName(), columns,
                            new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                            new ArrayList<CounterSuperColumn>(0)), m, CassandraUtilities.getEntity(e), relationNames,
                            isRelational);
                    entities.add(e);
                }
            }
        }
        catch (Exception e)
        {

            log.error("Error while populating data for relations of column family {}, Caused by: .", m.getTableName(),
                    e);
            throw new KunderaException(e);
        }
        return entities;
    }

    /** Query related methods */

    @Override
    public List executeQuery(Class clazz, List<String> relationalField, boolean isNative, String cqlQuery)
    {
        return super.executeSelectQuery(clazz, relationalField, dataHandler, isNative, cqlQuery);
    }

    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult, List<String> columns)
    {
        List<Object> entities = new ArrayList<Object>();
        Connection conn = null;
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
                        asList.add(UTF8Type.instance.decompose(colName));
                    }
                }
                slicePredicate.setColumn_names(asList);
            }
            else
            {
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                sliceRange.setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                slicePredicate.setSlice_range(sliceRange);
            }
            conn = getConnection();

            if (ixClause.isEmpty())
            {
                KeyRange keyRange = new KeyRange(maxResult);
                keyRange.setStart_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);

                if (m.isCounterColumnType())
                {
                    List<KeySlice> ks = conn.getClient().get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, keyRange, getConsistencyLevel());
                    entities = onCounterColumn(m, isRelation, relations, ks);

                }
                else
                {
                    List<KeySlice> keySlices = conn.getClient().get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, keyRange, getConsistencyLevel());

                    if (m.getType().isSuperColumnFamilyMetadata())
                    {
                        Map<ByteBuffer, List<SuperColumn>> qResults = ThriftDataResultHelper.transformThriftResult(
                                ColumnFamilyType.SUPER_COLUMN, keySlices, null);
                        entities = new ArrayList<Object>(qResults.size());
                        computeEntityViaSuperColumns(m, isRelation, relations, entities, qResults);
                    }
                    else
                    {
                        Map<ByteBuffer, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
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
                    List<KeySlice> keySlices = conn.getClient().get_indexed_slices(new ColumnParent(m.getTableName()),
                            ix, slicePredicate, getConsistencyLevel());

                    Map<ByteBuffer, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
                            ColumnFamilyType.COLUMN, keySlices, null);
                    // iterate through complete map and populate.
                    entities = new ArrayList<Object>(qResults.size());

                    computeEntityViaColumns(m, isRelation, relations, entities, qResults);
                }
            }
        }
        catch (InvalidRequestException irex)
        {
            log.error("Error during executing find of column family {}, Caused by: .", m.getTableName(), irex);
            throw new PersistenceException(irex);
        }
        catch (UnavailableException uex)
        {
            log.error("Error during executing find of column family {}, Caused by: .", m.getTableName(), uex);
            throw new PersistenceException(uex);
        }
        catch (TimedOutException tex)
        {
            log.error("Error during executing find of column family {}, Caused by: .", m.getTableName(), tex);
            throw new PersistenceException(tex);
        }
        catch (TException tex)
        {
            log.error("Error during executing find of column family {}, Caused by: .", m.getTableName(), tex);
            throw new PersistenceException(tex);
        }
        finally
        {

            releaseConnection(conn);
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
            List<String> columns, List<IndexExpression> conditions, int maxResults) throws Exception
    {
        SlicePredicate slicePredicate = new SlicePredicate();

        if (columns != null && !columns.isEmpty())
        {
            List asList = new ArrayList(32);
            for (String colName : columns)
            {
                if (colName != null)
                {
                    asList.add(UTF8Type.instance.decompose(colName));
                }
            }
            slicePredicate.setColumn_names(asList);
        }
        else
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            sliceRange.setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            slicePredicate.setSlice_range(sliceRange);
        }

        KeyRange keyRange = new KeyRange(maxResults);
        keyRange.setStart_key(minVal == null ? "".getBytes() : minVal);
        keyRange.setEnd_key(maxVal == null ? "".getBytes() : maxVal);
        ColumnParent cp = new ColumnParent(m.getTableName());

        if (conditions != null && !conditions.isEmpty())
        {
            keyRange.setRow_filter(conditions);
            keyRange.setRow_filterIsSet(true);
        }

        Connection conn = getConnection();

        List<KeySlice> keys = conn.getClient().get_range_slices(cp, slicePredicate, keyRange, getConsistencyLevel());

        releaseConnection(conn);

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

    protected Connection getConnection()
    {
        Connection connection = clientFactory.getConnection(pool);
        return connection;
    }

    /**
     * Return cassandra client instance.
     * 
     * @param connection
     * @return
     */
    protected Cassandra.Client getConnection(Object connection)
    {
        if (connection != null)
        {
            return ((Connection) connection).getClient();
        }

        throw new KunderaException("Invalid configuration!, no available pooled connection found for:"
                + this.getClass().getSimpleName());
    }

    protected void releaseConnection(Object conn)
    {
        clientFactory.releaseConnection(((Connection) conn).getPool(), ((Connection) conn).getClient());
    }

    @Override
    public Long generate(TableGeneratorDiscriptor discriptor)
    {
        return getGeneratedValue(discriptor, getPersistenceUnit());
    }

    @Override
    public Object generate()
    {
        return super.getAutoGeneratedValue();
    }
}
