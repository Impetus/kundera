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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
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
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.index.InvertedIndexHandler;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

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
        this.dataHandler = new ThriftDataHandler();
        setCassandraClient();
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
            // Create Insertion List
            List<Mutation> insertion_list = new ArrayList<Mutation>();

            for (Object key : joinTableRecords.keySet())
            {
                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(entityMetadata.getIdColumn().getField());
                byte[] rowKey = accessor.toBytes(key);          
                
                Set<Object> values = joinTableRecords.get(key);
                List<Column> columns = new ArrayList<Column>();
                for (Object value : values)
                {
                    Column column = new Column();
                    column.setName(PropertyAccessorFactory.STRING.toBytes(invJoinColumnName + "_" + (String) value));
                    column.setValue(PropertyAccessorFactory.STRING.toBytes((String) value));
                    column.setTimestamp(System.currentTimeMillis());

                    columns.add(column);
                    
                    Mutation mut = new Mutation();
                    mut.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                    insertion_list.add(mut);
                }

                createIndexesOnColumns(joinTableName, poolName, columns);
                
                // Create Mutation Map
                Map<String, List<Mutation>> columnFamilyValues = new HashMap<String, List<Mutation>>();
                columnFamilyValues.put(joinTableName, insertion_list);
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
        
        
        //TODO: Implement this
        
        return entities;
    }
    

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        //TODO: Implement this
        
        return null;
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

        List<Column> columns = new ArrayList<Column>();
        for (ColumnOrSuperColumn result : results) {     
          Column column = result.column;     
          columns.add(column);
        }      
        
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
        
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        ColumnFamilyStore cfs = Table.open(metadata.getSchema()).getColumnFamilyStore(tableName);

        //List<Row> rows = cfs.scan(ix, range, filter);
        
        

        Map<Bytes, List<Column>> qResults = null; 
            //selector.getIndexedColumns(tableName, ix, slicePredicate, consistencyLevel);

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

    @Override
    public List<Object> findByRelation(String colName, String colValue, Class entityClazz)
    {
        return null;
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
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        if (!isOpen())
        {
            throw new PersistenceException("ThriftClient is closed.");
        }
        
        try
        {
            cassandra_client.set_keyspace(schemaName);         
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
        return null;
    }

    @Override
    public Class<CassQuery> getQueryImplementor()
    {
        return null;
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

}
