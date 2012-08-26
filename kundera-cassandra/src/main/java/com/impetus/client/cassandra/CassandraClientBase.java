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
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;

import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;

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
    
    public Object find(Class entityClass, Object rowId)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        return find(entityClass, entityMetadata, rowId != null ? rowId : null, relationNames);
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
                    rowId != null ? rowId : null);
        }
        catch (Exception e)
        {
            log.error("Error on retrieval" + e.getMessage());
            throw new PersistenceException(e);
        }

        return result != null & !result.isEmpty() ? result.get(0) : null;
    }
    
    protected abstract List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            Object... rowIds);
    

}
