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
package com.impetus.client.cassandra;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnOrSuperColumnHelper;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Base Class for all Cassandra Clients Contains methods that are applicable to
 * (bot not specific to) different Cassandra clients.
 * 
 * @author amresh.singh
 */
public abstract class CassandraClientBase extends ClientBase
{

    /** log for this class. */
    private static Log log = LogFactory.getLog(CassandraClientBase.class);

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
    protected Column populateFkey(String rlName, String rlValue, long timestamp) throws PropertyAccessException
    {
        Column col = new Column();
        col.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        col.setValue(rlValue.getBytes());
        col.setTimestamp(timestamp);
        return col;
    }

    protected List<Object> onCounterColumn(EntityMetadata m, boolean isRelation, List<String> relations,
            List<KeySlice> ks)
    {
        List<Object> entities;
        if (m.getType().isSuperColumnFamilyMetadata())
        {
            Map<Bytes, List<CounterSuperColumn>> qCounterSuperColumnResults = ColumnOrSuperColumnHelper
                    .transformKeySlices(ks, ColumnOrSuperColumnHelper.COUNTER_SUPER_COLUMN);
            entities = new ArrayList<Object>(qCounterSuperColumnResults.size());

            // populateDataForSuperCounter(m, qCounterSuperColumnResults,
            // entities, isRelation, relations);

            for (Bytes key : qCounterSuperColumnResults.keySet())
            {
                List<CounterSuperColumn> counterSuperColumns = qCounterSuperColumnResults.get(key);
                try
                {
                    ThriftRow tr = new ThriftRow(ByteBufferUtil.string(key.getBytes(), Charset.forName("UTF-8")),
                            m.getTableName(), new ArrayList<Column>(0), new ArrayList<SuperColumn>(0),
                            new ArrayList<CounterColumn>(0), counterSuperColumns);
                    entities.add(getDataHandler().populateEntity(tr, m, relations, isRelation));
                }
                catch (CharacterCodingException ccex)
                {
                    log.error("Error during executing find, Caused by :" + ccex.getMessage());
                    throw new PersistenceException(ccex);
                }

            }

        }
        else
        {

            Map<Bytes, List<CounterColumn>> qCounterColumnResults = ColumnOrSuperColumnHelper.transformKeySlices(ks,
                    ColumnOrSuperColumnHelper.COUNTER_COLUMN);
            entities = new ArrayList<Object>(qCounterColumnResults.size());

            // populateDataForCounter(m, qCounterColumnResults, entities,
            // isRelation, relations, dataHandler);
            for (Bytes key : qCounterColumnResults.keySet())
            {
                List<CounterColumn> counterColumns = qCounterColumnResults.get(key);
                try
                {
                    ThriftRow tr = new ThriftRow(ByteBufferUtil.string(key.getBytes(), Charset.forName("UTF-8")),
                            m.getTableName(), new ArrayList<Column>(0), new ArrayList<SuperColumn>(0), counterColumns,
                            new ArrayList<CounterSuperColumn>(0));
                    entities.add(getDataHandler().populateEntity(tr, m, relations, isRelation));
                }
                catch (CharacterCodingException ccex)
                {
                    log.error("Error during executing find, Caused by :" + ccex.getMessage());
                    throw new PersistenceException(ccex);
                }

            }
        }
        return entities;
    }

    protected void computeEntityViaColumns(EntityMetadata m, boolean isRelation, List<String> relations,
            List<Object> entities, Map<Bytes, List<Column>> qResults)
    {
        for (Bytes key : qResults.keySet())
        {
            List<Column> columns = qResults.get(key);
            ThriftRow tr = new ThriftRow(PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(),
                    key.toByteArray()), m.getTableName(), columns, new ArrayList<SuperColumn>(0),
                    new ArrayList<CounterColumn>(0), new ArrayList<CounterSuperColumn>(0));
            entities.add(getDataHandler().populateEntity(tr, m, relations, isRelation));

        }
    }

    /**
     * Adds relation foreign key values as thrift column/ value to thrift row
     * 
     * @param metadata
     * @param tf
     * @param relations
     */
    protected void addRelationsToThriftRow(EntityMetadata metadata, ThriftRow tf, List<RelationHolder> relations)
    {
        long timestamp = System.currentTimeMillis();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                String linkName = rh.getRelationName();
                String linkValue = rh.getRelationValue();

                if (linkName != null && linkValue != null)
                {
                    // if (metadata.getEmbeddedColumnsAsList().isEmpty())
                    if (metaModel.getEmbeddables(metadata.getEntityClazz()).isEmpty()/*
                                                                                      * metadata
                                                                                      * .
                                                                                      * getEmbeddedColumnsAsList
                                                                                      * (
                                                                                      * )
                                                                                      * .
                                                                                      * isEmpty
                                                                                      * (
                                                                                      * )
                                                                                      */)
                    {
                        if (metadata.isCounterColumnType())
                        {
                            CounterColumn col = populateCounterFkey(linkName, linkValue);
                            tf.addCounterColumn(col);
                        }
                        else
                        {
                            Column col = populateFkey(linkName, linkValue, timestamp);
                            tf.addColumn(col);
                        }

                    }
                    else
                    {
                        if (metadata.isCounterColumnType())
                        {
                            CounterSuperColumn counterSuperColumn = new CounterSuperColumn();
                            counterSuperColumn.setName(linkName.getBytes());
                            CounterColumn column = populateCounterFkey(linkName, linkValue);
                            counterSuperColumn.addToColumns(column);
                            tf.addCounterSuperColumn(counterSuperColumn);
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
    }

    /**
     * @param rlName
     * @param rlValue
     * @return
     */
    private CounterColumn populateCounterFkey(String rlName, String rlValue)
    {
        CounterColumn counterCol = new CounterColumn();
        counterCol.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        counterCol.setValue(new Long(rlValue));
        return counterCol;
    }

    /**
     * Deletes record for given primary key from counter column family
     * 
     * @param pKey
     * @param metadata
     */
    protected void deleteRecordFromCounterColumnFamily(Object pKey, EntityMetadata metadata,
            ConsistencyLevel consistencyLevel)
    {
        ColumnPath path = new ColumnPath(metadata.getTableName());

        IPooledConnection conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(metadata.getPersistenceUnit());
            Cassandra.Client cassandra_client = conn.getAPI();
            cassandra_client.set_keyspace(metadata.getSchema());
            cassandra_client.remove_counter(
                    (CassandraUtilities.toBytes(pKey, metadata.getIdAttribute().getJavaType())).getBytes(), path,
                    consistencyLevel);

        }
        catch (InvalidRequestException ire)
        {
            log.error("Error during executing delete, Caused by :" + ire.getMessage());
            throw new PersistenceException(ire);
        }
        catch (UnavailableException ue)
        {
            log.error("Error during executing delete, Caused by :" + ue.getMessage());
            throw new PersistenceException(ue);
        }
        catch (TimedOutException toe)
        {
            log.error("Error during executing delete, Caused by :" + toe.getMessage());
            throw new PersistenceException(toe);
        }
        catch (TException te)
        {
            log.error("Error during executing delete, Caused by :" + te.getMessage());
            throw new PersistenceException(te);
        }
        finally
        {
            PelopsUtils.releaseConnection(conn);
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
    protected void createIndexesOnColumns(String tableName, String poolName, List<Column> columns)
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

            if (columnFamilyDefToUpdate == null)
            {
                throw new PersistenceException("Join table does not exist in database");
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
                columnDef.setValidation_class(BytesType.class.getSimpleName());
                columnDef.setIndex_type(IndexType.KEYS);

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

    public Object find(Class entityClass, Object rowId)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        return find(entityClass, entityMetadata, rowId, relationNames);
    }

    public <E> List<E> findAll(Class<E> entityClass, Object... rowIds)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        List<E> results = new ArrayList<E>();
        results = find(entityClass, entityMetadata.getRelationNames(), entityMetadata.getRelationNames() != null
                && !entityMetadata.getRelationNames().isEmpty(), entityMetadata, rowIds);
        return results.isEmpty() ? null : results;
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
            result = (List<Object>) find(clazz, relationNames, relationNames != null, metadata, rowId);
        }
        catch (Exception e)
        {
            log.error("Error on retrieval" + e.getMessage());
            throw new PersistenceException(e);
        }

        return result != null & !result.isEmpty() ? result.get(0) : null;
    }

    public <E> List<E> find(Class<E> entityClass, Map<String, String> superColumnMap, CassandraDataHandler dataHandler)
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
                E e = (E) dataHandler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                        new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
                if (e != null)
                {
                    entities.add(e);
                }
            }
        }
        catch (Exception e)
        {
            throw new KunderaException(e);
        }
        return entities;
    }

    public List executeQuery(String cqlQuery, Class clazz, List<String> relationalField,
            CassandraDataHandler dataHandler)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz);
        CqlResult result = null;
        List returnedEntities = null;
        IPooledConnection conn = null;
        try
        {
            conn = PelopsUtils.getCassandraConnection(entityMetadata.getPersistenceUnit());
            Cassandra.Client cassandra_client = conn.getAPI();
            cassandra_client.set_keyspace(entityMetadata.getSchema());

            result = cassandra_client.execute_cql_query(ByteBufferUtil.bytes(cqlQuery),
                    org.apache.cassandra.thrift.Compression.NONE);
            if (result != null && (result.getRows() != null || result.getRowsSize() > 0))
            {
                returnedEntities = new ArrayList<Object>(result.getRowsSize());
                Iterator<CqlRow> iter = result.getRowsIterator();
                while (iter.hasNext())
                {
                    CqlRow row = iter.next();
                    String rowKey = Bytes.toUTF8(row.getKey());
                    ThriftRow thriftRow = null;
                    if (entityMetadata.isCounterColumnType())
                    {
                        log.info("Native query is not permitted on counter column returning null ");
                        return null;
                    }
                    else
                    {
                        thriftRow = new ThriftRow(rowKey, entityMetadata.getTableName(), row.getColumns(),
                                new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                                new ArrayList<CounterSuperColumn>(0));
                    }

                    Object entity = dataHandler.populateEntity(thriftRow, entityMetadata, relationalField,
                            relationalField != null && !relationalField.isEmpty());

                    if (entity != null)
                    {
                        returnedEntities.add(entity);
                    }
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
        finally
        {
            PelopsUtils.releaseConnection(conn);
        }
        return returnedEntities;

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
    protected void populateData(EntityMetadata m, Map<Bytes, List<Column>> qResults, List<Object> entities,
            boolean isRelational, List<String> relationNames, CassandraDataHandler dataHandler)
    {
        if (m.getType().isSuperColumnFamilyMetadata())
        {
            Set<Bytes> primaryKeys = qResults.keySet();

            if (primaryKeys != null && !primaryKeys.isEmpty())
            {
                Object[] rowIds = new Object[primaryKeys.size()];
                int i = 0;
                for (Bytes b : primaryKeys)
                {
                    rowIds[i] = PropertyAccessorHelper.getObject(b, (Field) m.getIdAttribute().getJavaMember());
                    i++;
                }
                entities.addAll(findAll(m.getEntityClazz(), rowIds));
            }

        }
        else
        {
            Iterator<Bytes> rowIter = qResults.keySet().iterator();
            while (rowIter.hasNext())
            {
                Bytes rowKey = rowIter.next();
                List<Column> columns = qResults.get(rowKey);
                try
                {
                    Object id = PropertyAccessorHelper
                            .getObject(m.getIdAttribute().getJavaType(), rowKey.toByteArray());

                    Object e = dataHandler.populateEntity(new ThriftRow(id, m.getTableName(), columns,
                            new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                            new ArrayList<CounterSuperColumn>(0)), m, relationNames, isRelational);

                    // Object e = dataHandler
                    // .fromColumnThriftRow(m.getEntityClazz(), m, new
                    // ThriftRow(id, m.getTableName(), columns,
                    // null, null, null), relationNames, isRelational);
                    if (e != null)
                    {
                        entities.add(e);
                    }
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

    }

    /**
     * @param m
     * @param isWrapReq
     * @param relations
     * @param keys
     * @return
     * @throws Exception
     */
    protected List populateEntitiesFromKeySlices(EntityMetadata m, boolean isWrapReq, List<String> relations,
            List<KeySlice> keys, CassandraDataHandler dataHandler) throws Exception
    {
        List results;
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        // List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Set<String> superColumnAttribs = metaModel.getEmbeddables(m.getEntityClazz()).keySet();
        results = new ArrayList(keys.size());

        ThriftDataResultHelper dataGenerator = new ThriftDataResultHelper();
        for (KeySlice key : keys)
        {
            List<ColumnOrSuperColumn> columns = key.getColumns();

            byte[] rowKey = key.getKey();

            Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), rowKey);
            Map<ByteBuffer, List<ColumnOrSuperColumn>> data = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>(1);
            data.put(ByteBuffer.wrap(rowKey), columns);
            ThriftRow tr = new ThriftRow();
            tr.setId(id);
            tr.setColumnFamilyName(m.getTableName());
            tr = dataGenerator.translateToThriftRow(data, m.isCounterColumnType(), m.getType(), tr);
            results.add(dataHandler.populateEntity(tr, m, relations, isWrapReq));
        }
        return results;
    }

    public abstract List find(Class entityClass, List<String> relationNames, boolean isWrapReq,
            EntityMetadata metadata, Object... rowIds);

    protected abstract List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames);

    /** Query related methods */
    public abstract List executeQuery(String cqlQuery, Class clazz, List<String> relationalField);

    public abstract List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult);

    public abstract List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq,
            List<String> relations) throws Exception;

    public abstract List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Queue<FilterClause> filterClauseQueue);

    public abstract List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames,
            List<IndexClause> conditions, int maxResult);

    protected abstract CassandraDataHandler getDataHandler();
}
