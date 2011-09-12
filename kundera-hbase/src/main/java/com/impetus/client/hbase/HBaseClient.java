/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.hbase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.client.hbase.admin.DataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.Constants;
import com.impetus.kundera.db.DataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient implements com.impetus.kundera.Client
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseClient.class);
    
    /** The contact node. */
    String contactNode;

    /** The default port. */
    String defaultPort;

    /** The handler. */
    private DataHandler handler;

    /** The is connected. */
    private boolean isConnected;

    /** The em. */
    private EntityManager em;


    /**
     * Writes an entity data into HBase store
     */
    @Override
    public void writeData(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) throws Exception
    {
        String dbName = m.getSchema();          //Has no meaning for HBase, not used
        String tableName = m.getTableName();        
        
        List<String> columnFamilyNames = new ArrayList<String>();
        
        //If this entity has columns(apart from embedded objects, they will be treated as column family)
        List<Column> columns = m.getColumnsAsList();     
        if(columns != null && ! columns.isEmpty()) {
            columnFamilyNames.addAll(m.getColumnFieldNames());
        }
        
        //All relationships are maintained as special Foreign key column by Kundera in a newly created column family 
        List<Relation> relations = m.getRelations();  
        if(!relations.isEmpty()) {
            columnFamilyNames.add(Constants.TO_ONE_SUPER_COL_NAME);
        }
        
        //Check whether this table exists, if not create it
        columnFamilyNames.addAll(m.getEmbeddedColumnFieldNames());
        handler.createTableIfDoesNotExist(tableName, columnFamilyNames.toArray(new String[0]));
        
        //Write data to HBase
        handler.writeData(tableName, m, e);     
        
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E loadData(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, String rowKey,
            EntityMetadata m) throws Exception
    {
        //columnFamily has a different meaning for HBase, so it won't be used here
        String tableName = m.getTableName();
        E e = handler.readData(tableName, clazz, m, rowKey);
        return e;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> loadData(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily,
            EntityMetadata m, String... keys) throws Exception
    {
        List<E> entities = new ArrayList<E>();
        for (String rowKey : keys)
        {
            E e = handler.readData(m.getTableName(), clazz, m, rowKey);
            entities.add(e);
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, com.impetus.kundera.metadata.EntityMetadata,
     * java.util.Queue)
     */
    public <E> List<E> loadData(EntityManagerImpl em, EntityMetadata m, Query query) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    } 

    
    @Override
    public <E> List<E> loadData(EntityManager em, Class<E> clazz, EntityMetadata m, Map<String, String> col,
            String keyspace, String family) throws Exception
    {
        List<E> entities = new ArrayList<E>();
        Map<String, Field> columnFamilyNameToFieldMap = MetadataUtils.createSuperColumnsFieldMap(m);
        for (String columnFamilyName : col.keySet())
        {
            String entityId = col.get(columnFamilyName);
            E e = handler.readData(m.getTableName(), clazz, m, entityId);
            
            Field columnFamilyField = columnFamilyNameToFieldMap.get(columnFamilyName);
            Object columnFamilyValue = PropertyAccessorHelper.getObject(e, columnFamilyField);
            if(Collection.class.isAssignableFrom(columnFamilyField.getType())) {
                entities.addAll((Collection)columnFamilyValue);
            } else {
                entities.add((E)columnFamilyValue);
            }           
        }
        return entities;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#loadEmbeddedObjects(java.lang.String, java.lang.String, java.lang.String[])
     */
    @Override
    public Map<Bytes, List<SuperColumn>> loadEmbeddedObjects(String keyspace, String columnFamily, String... keys)
            throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#shutdown()
     */
    @Override
    public void shutdown()
    {
        handler.shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public void connect()
    {
        if (!isConnected)
        {
            handler = new HBaseDataHandler(contactNode, defaultPort);
            isConnected = true;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setContactNodes(java.lang.String[])
     */
    @Override
    public void setContactNodes(String... contactNodes)
    {
        this.contactNode = contactNodes[0];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setDefaultPort(int)
     */
    @Override
    public void setDefaultPort(int defaultPort)
    {
        this.defaultPort = String.valueOf(defaultPort);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    @Override
    public void delete(String keyspace, String columnFamily, String rowId) throws Exception
    {
        throw new RuntimeException("TODO:not yet supported");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setKeySpace(String keySpace)
    {
        // TODO not required, Keyspace not applicable to Hbase
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#getType()
     */
    @Override
    public DBType getType()
    {
        return DBType.HBASE;
    }

    @Override
    public DataAccessor getDataAccessor()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Indexer getIndexer()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Query getQuery()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
