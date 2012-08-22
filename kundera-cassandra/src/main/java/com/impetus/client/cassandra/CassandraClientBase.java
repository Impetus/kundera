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

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
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
import org.scale7.cassandra.pelops.Pelops;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Base Class for all Cassandra Clients
 * Contains methods that are applicable to (bot not specific to) different Cassandra clients.
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
    
    /**
     * Adds relation foreign key values as thrift column/ value to thrift row
     * 
     * @param metadata
     * @param tf
     * @param relations
     */
    protected void addRelationsToThriftRow(EntityMetadata metadata, ThriftRow tf,
            List<RelationHolder> relations)
    {
        long timestamp = System.currentTimeMillis();
        
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
     * @param pKey
     * @param metadata
     */
    protected void deleteRecordFromCounterColumnFamily(Object pKey, EntityMetadata metadata, ConsistencyLevel consistencyLevel, Cassandra.Client cassandra_client)
    {
        ColumnPath path = new ColumnPath(metadata.getTableName());
        
        try
        {
            cassandra_client.remove_counter(ByteBuffer.wrap(pKey.toString().getBytes()), path, consistencyLevel);              
            
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
        return find(entityClass, entityMetadata, rowId != null ? rowId.toString() : null, relationNames);
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
            result = (List<Object>) find(clazz, relationNames, relationNames != null, metadata,
                    rowId != null ? rowId.toString() : null);
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
    protected void populateDataForCounter(EntityMetadata m, Map<Bytes, List<CounterColumn>> qCounterResults,
            List<Object> entities, boolean isRelational, List<String> relationNames, CassandraDataHandler dataHandler)
    {
        Iterator<Bytes> rowIter = qCounterResults.keySet().iterator();
        while (rowIter.hasNext())
        {
            Bytes rowKey = rowIter.next();
            List<CounterColumn> counterColumns = qCounterResults.get(rowKey);
            try
            {
                Object e = dataHandler.fromCounterColumnThriftRow(m.getEntityClazz(), m,
                        new ThriftRow(Bytes.toUTF8(rowKey.toByteArray()), m.getTableName(), null, null,
                                counterColumns, null), relationNames, isRelational);
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

    /**
     * @param m
     * @param qCounterResults
     * @param entities
     * @param isRelational
     * @param relationNames
     */
    protected void populateDataForSuperCounter(EntityMetadata m, Map<Bytes, List<CounterSuperColumn>> qCounterResults,
            List<Object> entities, boolean isRelational, List<String> relationNames)
    {
        Set<Bytes> primaryKeys = qCounterResults.keySet();

        if (primaryKeys != null && !primaryKeys.isEmpty())
        {
            Object[] rowIds = new Object[primaryKeys.size()];
            int i = 0;
            for (Bytes b : primaryKeys)
            {
                rowIds[i] = Bytes.toUTF8(b.toByteArray());
                i++;
            }
            entities.addAll(findAll(m.getEntityClazz(), rowIds));
        }
    }
    
    
    public List executeQuery(String cqlQuery, Class clazz, List<String> relationalField, Cassandra.Client cassandra_client, CassandraDataHandler dataHandler)
    {
        
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz);
        CqlResult result = null;
        List returnedEntities = null;
        try
        {
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
                        thriftRow = new ThriftRow(rowKey, entityMetadata.getTableName(), row.getColumns(), null, null,
                                null);
                    }

                    Object entity = dataHandler.fromColumnThriftRow(clazz, entityMetadata, thriftRow, relationalField,
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
                    rowIds[i] = Bytes.toUTF8(b.toByteArray());
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
                    Object e = dataHandler.fromColumnThriftRow(m.getEntityClazz(), m,
                            new ThriftRow(Bytes.toUTF8(rowKey.toByteArray()), m.getTableName(), columns, null,
                                    null, null), relationNames, isRelational);
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
    
    
    public abstract List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds);
    
    protected abstract List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames);    
    
    /** Query related methods*/
    public abstract List executeQuery(String cqlQuery, Class clazz, List<String> relationalField);
    
    public abstract List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult);
    
    public abstract List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations)
    throws Exception;
    
    public abstract List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Queue<FilterClause> filterClauseQueue);
    
    public abstract List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
            int maxResult);

}
