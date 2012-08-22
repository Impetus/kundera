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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnOrSuperColumnHelper;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.db.SearchResult;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Kundera Client implementation for Cassandra using Thrift library
 * 
 * @author amresh.singh
 */
public class ThriftClient extends CassandraClientBase implements Client<CassQuery>
{
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private ThriftDataHandler dataHandler;

    /** Handler for Inverted indexing */
    private InvertedIndexHandler invertedIndexHandler;

    /** The reader. */
    private EntityReader reader;

    /** The timestamp. */
    private long timestamp;

    /** Handle to Cassandra Client provided by thrift */
    private Cassandra.Client cassandra_client;

    public ThriftClient(IndexManager indexManager, EntityReader reader, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;        
        setCassandraClient();
        this.dataHandler = new ThriftDataHandler(this.cassandra_client);        
        this.invertedIndexHandler = new ThriftInvertedIndexHandler(this.cassandra_client);
        this.reader = reader;
    }
    
    @Override
    public void persist(Node node)
    {
        super.persist(node);
    }
    
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }

        // check for counter column
        if (isUpdate && entityMetadata.isCounterColumnType())
        {
            throw new UnsupportedOperationException(" Merge is not permitted on counter column! ");
        }

        ThriftRow tf = null;
        try
        {
            String columnFamily = entityMetadata.getTableName();
            tf = dataHandler.toThriftRow(entity, id.toString(), entityMetadata, columnFamily);
            timestamp = System.currentTimeMillis();
        }
        catch (Exception e)
        {
            log.error("Error during persist, Caused by:" + e.getMessage());
            throw new KunderaException(e);
        }

        addRelationsToThriftRow(entityMetadata, tf, rlHolders);

        try
        {
            //setCassandraClient();
            cassandra_client.set_keyspace(entityMetadata.getSchema());
            byte[] rowKey = PropertyAccessorHelper.get(entity, entityMetadata.getIdColumn().getField());
            String columnFamily = entityMetadata.getTableName();
            // Create Insertion List
            List<Mutation> insertion_list = new ArrayList<Mutation>();

            
            /*********** Handling for counter column family ************/
            
            if (entityMetadata.isCounterColumnType())
            {
                List<CounterColumn> thriftCounterColumns = tf.getCounterColumns();
                List<CounterSuperColumn> thriftCounterSuperColumns = tf.getCounterSuperColumns();

                if (thriftCounterColumns != null && !thriftCounterColumns.isEmpty())
                {
                    for (CounterColumn column : thriftCounterColumns)
                    {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_column(column));
                        insertion_list.add(mut);
                    }
                }

                if (thriftCounterSuperColumns != null && !thriftCounterSuperColumns.isEmpty())
                {
                    for (CounterSuperColumn sc : thriftCounterSuperColumns)
                    {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_super_column(sc));
                        insertion_list.add(mut);
                    }
                }
            }
            else
            /********* Handling for column family and super column family *********/
            {
                List<Column> thriftColumns = tf.getColumns();
                List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();

                // Populate Insertion list for columns
                if (thriftColumns != null && !thriftColumns.isEmpty())
                {

                    for (Column column : thriftColumns)
                    {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                        insertion_list.add(mut);
                    }
                }

                // Populate Insertion list for super columns
                if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
                {
                    for (SuperColumn superColumn : thriftSuperColumns)
                    {
                        Mutation mut = new Mutation();
                        mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(superColumn));
                        insertion_list.add(mut);
                    }
                }                
            }            
            
            // Create Mutation Map
            Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
            columnFamilyValues.put(columnFamily, insertion_list);
            Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

            // Write Mutation map to database
            cassandra_client.batch_mutate(mulationMap, consistencyLevel);            
        }
        catch (InvalidRequestException e)
        {
            log.error(e.getMessage());
            throw new KunderaException(e);
        }
        catch (TException e)
        {
            log.error(e.getMessage());
            throw new KunderaException(e);
        }
        catch (UnavailableException e)
        {
            log.error(e.getMessage());
            throw new KunderaException(e);
        }
        catch (TimedOutException e)
        {
            log.error(e.getMessage());
            throw new KunderaException(e);
        }

    }
    
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit());        

        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();
        
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(joinTableData.getEntityClass());
        
        try
        {
            cassandra_client.set_keyspace(entityMetadata.getSchema());       
            

            for (Object key : joinTableRecords.keySet())
            {
                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(entityMetadata.getIdColumn().getField());
                byte[] rowKey = accessor.toBytes(key);          
                
                Set<Object> values = joinTableRecords.get(key);
                List<Column> columns = new ArrayList<Column>();
                
                // Create Insertion List
                List<Mutation> insertionList = new ArrayList<Mutation>();
                
                for (Object value : values)
                {
                    Column column = new Column();
                    column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName + "_" + (String) value));
                    column.setValue(PropertyAccessorFactory.STRING.toBytes((String) value));
                    column.setTimestamp(System.currentTimeMillis());

                    columns.add(column);
                    
                    Mutation mut = new Mutation();
                    mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                    insertionList.add(mut);
                }

                createIndexesOnColumns(joinTableName, poolName, columns);
                
                // Create Mutation Map
                Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                columnFamilyValues.put(joinTableName, insertionList);
                Map<ByteBuffer, Map<String, List<Mutation>>> mulationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                mulationMap.put(ByteBuffer.wrap(rowKey), columnFamilyValues);

                // Write Mutation map to database
                cassandra_client.batch_mutate(mulationMap, consistencyLevel);     
                
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
    }
    
    @Override
    protected void indexNode(Node node, EntityMetadata entityMetadata)
    {
        super.indexNode(node, entityMetadata);

        // Write to inverted index table if applicable
        //setCassandraClient();
        invertedIndexHandler.writeToInvertedIndexTable(node, entityMetadata, getPersistenceUnit(), consistencyLevel,
                dataHandler);

    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        return super.find(entityClass, key);
    }
    
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        return super.findAll(entityClass, keys);
    }
    
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
                    consistencyLevel, rowIds);
        }
        catch (Exception e)
        {
            throw new KunderaException(e);
        }

        return entities;        
    }
    

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return super.find(entityClass, embeddedColumnMap, dataHandler);
    }
    
    @Override
    public final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames)
    {
        if (!isOpen())
            throw new PersistenceException("ThriftClient is closed.");
        
        byte[] rowKey = rowId.getBytes();
        
        SlicePredicate predicate = new SlicePredicate(); 
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for(String superColumnName : superColumnNames) {
            columnNames.add(ByteBuffer.wrap(superColumnName.getBytes()));
        }
        
        predicate.setColumn_names(columnNames);

        ColumnParent parent = new ColumnParent(columnFamily); 
        List<ColumnOrSuperColumn> coscList;
        try
        {
            coscList = cassandra_client.get_slice(ByteBuffer.wrap(rowKey), parent, predicate, consistencyLevel);
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

        List<SuperColumn> superColumns = ThriftDataResultHelper.transformThriftResult(coscList, ColumnFamilyType.SUPER_COLUMN);       
        return superColumns;        
    }
    
    @Override
    public <E> List<E> getColumnsById(String tableName, String pKeyColumnName, String columnName, String pKeyColumnValue)
    {        
       
        byte[] rowKey = pKeyColumnValue.getBytes();
        
        SlicePredicate predicate = new SlicePredicate(); 
        SliceRange sliceRange = new SliceRange(); 
        sliceRange.setStart(new byte[0]); 
        sliceRange.setFinish(new byte[0]); 
        predicate.setSlice_range(sliceRange);

        ColumnParent parent = new ColumnParent(tableName); 
        List<ColumnOrSuperColumn> results;
        try
        {
            results = cassandra_client.get_slice(ByteBuffer.wrap(rowKey), parent, predicate, consistencyLevel);
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

        List<Column> columns = ThriftDataResultHelper.transformThriftResult(results, ColumnFamilyType.COLUMN);        
        
        List<E> foreignKeys = dataHandler.getForeignKeysFromJoinTable(columnName, columns);
        return foreignKeys;
    }

    @Override
    public Object[] findIdsByColumn(String tableName, String pKeyName, String columnName, Object columnValue,
            Class entityClazz)
    {       
        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClazz);
        String childIdStr = (String) columnValue;        
        IndexExpression ie = new IndexExpression(Bytes.fromUTF8(columnName + "_" + childIdStr).getBytes(),
                IndexOperator.EQ, Bytes.fromUTF8(childIdStr).getBytes());       
        IndexClause ix = Selector.newIndexClause(Bytes.EMPTY, 10000, ie);       
        
        
        List<Object> rowKeys = new ArrayList<Object>();        
        ColumnParent columnParent = new ColumnParent(tableName);
        try
        {
            List<KeySlice> keySlices = cassandra_client.get_indexed_slices(columnParent, ix, slicePredicate,
                    consistencyLevel);
            
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
        
        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }      
        
        return null;
    }    

    @Override
    public List<Object> findByRelation(String colName, String colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);       

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, 10000);
        List<Object> entities = null;
        
        IndexExpression ie = new IndexExpression(Bytes.fromUTF8(colName).getBytes(),
                IndexOperator.EQ, Bytes.fromUTF8(colValue).getBytes());       
        IndexClause ix = Selector.newIndexClause(Bytes.EMPTY, 10000, ie);   
        ColumnParent columnParent = new ColumnParent(m.getTableName());        
        
        List<KeySlice> keySlices;
        try
        {
            keySlices = cassandra_client.get_indexed_slices(columnParent, ix, slicePredicate,
                    consistencyLevel);
        }
        catch (InvalidRequestException e)
        {
            log.error("Error while finding relations. Details:" + e.getMessage());
            throw new KunderaException("Error while finding relations", e);
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
 
        if(keySlices != null) {
            entities = new ArrayList<Object>(keySlices.size());           
            populateData(m, keySlices, entities, false, null);            
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
        
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        try
        {
            cassandra_client.set_keyspace(metadata.getSchema());            
            
            if (metadata.isCounterColumnType())
            {
                deleteRecordFromCounterColumnFamily(pKey, metadata, consistencyLevel, cassandra_client);
            }
            else
            {
                ColumnPath path = new ColumnPath(metadata.getTableName());
                
                cassandra_client.remove(ByteBuffer.wrap(pKey.toString().getBytes()), path, System.currentTimeMillis(), consistencyLevel);
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
        
        // Delete from Lucene if applicable
        getIndexManager().remove(metadata, entity, pKey.toString());
        
        // Delete from Inverted Index if applicable        
        invertedIndexHandler.deleteRecordsFromIndexTable(entity, metadata, consistencyLevel);
    }
    
    @Override
    public void deleteByColumn(String tableName, String columnName, Object columnValue)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
        .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }
        
        try
        {
            cassandra_client.set_keyspace(keyspace);         
            ColumnPath path = new ColumnPath(tableName);        
            cassandra_client.remove(ByteBuffer.wrap(columnValue.toString().getBytes()), path, System.currentTimeMillis(), consistencyLevel);
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

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    private final boolean isOpen()
    {
        return !closed;
    }
    
    @Override
    public void close()
    {
        this.indexManager.flush();
        this.dataHandler = null;
        this.invertedIndexHandler = null;
        this.cassandra_client = null;

        closed = true;
    }
    
    private void setCassandraClient() {
        if(cassandra_client == null) {
            cassandra_client = PelopsUtils.getCassandraClient(persistenceUnit);
        }        
    }
    

    private void populateData(EntityMetadata m, List<KeySlice> keySlices, List<Object> entities,
            boolean isRelational, List<String> relationNames)
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
                for(KeySlice keySlice : keySlices) {
                    byte[] key = keySlice.getKey();                
                    List<ColumnOrSuperColumn> coscList = keySlice.getColumns();
                    
                    List<Column> columns = ThriftDataResultHelper.transformThriftResult(coscList, ColumnFamilyType.COLUMN);        
                    
                    Object e = dataHandler.fromColumnThriftRow(m.getEntityClazz(), m,
                            new ThriftRow(Bytes.toUTF8(key), m.getTableName(), columns, null,
                                    null, null), relationNames, isRelational);
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
        return super.executeQuery(cqlQuery, clazz, relationalField, cassandra_client, dataHandler);       
    }

    @Override
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations,
            int maxResult)
    {
        List<Object> entities = null;
        try
        {
            // ixClause can be 0,1 or more!
            SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);

            if (ixClause.isEmpty())
            {
                KeyRange keyRange = new KeyRange(maxResult);
                keyRange.setStart_key(Bytes.nullSafeGet(Bytes.fromUTF8("")));
                keyRange.setEnd_key(Bytes.nullSafeGet(Bytes.fromUTF8("")));

                if (m.isCounterColumnType())
                {

                    List<KeySlice> ks = cassandra_client.get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, keyRange, consistencyLevel);
                    if (m.getType().isSuperColumnFamilyMetadata())
                    {
                        Map<Bytes, List<CounterSuperColumn>> qCounterSuperColumnResults = ThriftDataResultHelper
                                .transformThriftResult(ColumnFamilyType.COUNTER_SUPER_COLUMN, ks);

                        entities = new ArrayList<Object>(qCounterSuperColumnResults.size());

                        populateDataForSuperCounter(m, qCounterSuperColumnResults, entities, isRelation, relations);
                    }
                    else
                    {

                        Map<Bytes, List<CounterColumn>> qCounterColumnResults = ThriftDataResultHelper
                                .transformThriftResult(ColumnFamilyType.COUNTER_COLUMN, ks);

                        entities = new ArrayList<Object>(qCounterColumnResults.size());

                        populateDataForCounter(m, qCounterColumnResults, entities, isRelation, relations, dataHandler);
                    }

                }
                else
                {

                    List<KeySlice> keySlices = cassandra_client.get_range_slices(new ColumnParent(m.getTableName()),
                            slicePredicate, keyRange, consistencyLevel);

                    Map<Bytes, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
                            ColumnFamilyType.COLUMN, keySlices);

                    entities = new ArrayList<Object>(qResults.size());
                    populateData(m, qResults, entities, isRelation, relations, dataHandler);
                }
            }
            else
            {
                entities = new ArrayList<Object>();
                for (IndexClause ix : ixClause)
                {
                    List<KeySlice> keySlices = cassandra_client.get_indexed_slices(new ColumnParent(m.getTableName()),
                            ix, slicePredicate, consistencyLevel);

                    Map<Bytes, List<Column>> qResults = ThriftDataResultHelper.transformThriftResult(
                            ColumnFamilyType.COLUMN, keySlices);
                    // iterate through complete map and
                    populateData(m, qResults, entities, isRelation, relations, dataHandler);
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
        return entities;
    }
    
    @Override
    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions,
            int maxResult)
    {
        return null;
    }  

    @Override
    public List findByRange(byte[] minVal, byte[] maxVal, EntityMetadata m, boolean isWrapReq, List<String> relations)
            throws Exception
    {
        return null;
    }

    @Override
    public List<SearchResult> searchInInvertedIndex(String columnFamilyName, EntityMetadata m,
            Queue<FilterClause> filterClauseQueue)
    {
        return  invertedIndexHandler.getSearchResults(m, filterClauseQueue,
                getPersistenceUnit(), consistencyLevel);  
    }

     

}
