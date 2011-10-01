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

import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.hbase.admin.DataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.DBType;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.EntityResolver;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.LuceneQuery;

/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient implements com.impetus.kundera.client.Client
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

    /** The index manager. */
    private IndexManager indexManager;

    private EntityResolver entityResolver;

    private String persistenceUnit;

    private Indexer indexer;

    /**
     * Writes an entity data into HBase store
     */
    @Override
    public void writeData(EnhancedEntity enhancedEntity) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity
                .getEntity().getClass());

        String dbName = entityMetadata.getSchema(); // Has no meaning for HBase,
                                                    // not used
        String tableName = entityMetadata.getTableName();

        List<String> columnFamilyNames = new ArrayList<String>();

        // If this entity has columns(apart from embedded objects, they will be
        // treated as column family)
        List<Column> columns = entityMetadata.getColumnsAsList();
        if (columns != null && !columns.isEmpty())
        {
            columnFamilyNames.addAll(entityMetadata.getColumnFieldNames());
        }

        // All relationships are maintained as special Foreign key column by
        // Kundera in a newly created column family
        List<Relation> relations = entityMetadata.getRelations();
        if (!relations.isEmpty())
        {
            columnFamilyNames.add(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME);
        }

        // Check whether this table exists, if not create it
        columnFamilyNames.addAll(entityMetadata.getEmbeddedColumnFieldNames());
        handler.createTableIfDoesNotExist(tableName, columnFamilyNames.toArray(new String[0]));

        // Write data to HBase
        handler.writeData(tableName, entityMetadata, enhancedEntity);

    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E loadData(Class<E> entityClass, String rowId) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getTableName();
        E e = (E) handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata, rowId);
        return e;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, java.lang.Class, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> loadData(Class<E> entityClass, String... rowIds) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        for (String rowKey : rowIds)
        {
            E e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                    rowKey);
            entities.add(e);
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, com.impetus.kundera.metadata.EntityMetadata,
     * java.util.Queue)
     */
    public <E> List<E> loadData(Query query) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public <E> List<E> loadData(Class<E> entityClass, Map<String, String> col) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        Map<String, Field> columnFamilyNameToFieldMap = MetadataUtils.createSuperColumnsFieldMap(entityMetadata);
        for (String columnFamilyName : col.keySet())
        {
            String entityId = col.get(columnFamilyName);
            E e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                    entityId);

            Field columnFamilyField = columnFamilyNameToFieldMap.get(columnFamilyName);
            Object columnFamilyValue = PropertyAccessorHelper.getObject(e, columnFamilyField);
            if (Collection.class.isAssignableFrom(columnFamilyField.getType()))
            {
                entities.addAll((Collection) columnFamilyValue);
            }
            else
            {
                entities.add((E) columnFamilyValue);
            }
        }
        return entities;
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
    public void delete(EnhancedEntity enhancedEntity) throws Exception
    {
        throw new RuntimeException("TODO:not yet supported");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setSchema(String keySpace)
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
    public Indexer getIndexer()
    {
        return indexer;
    }

    /**
     * Gets the index manager.
     * 
     * @return the indexManager
     */
    @Override
    public final IndexManager getIndexManager()
    {
        return indexManager;
    }

    @Override
    public Query getQuery(String ejbqlString)
    {
        return new LuceneQuery(this, ejbqlString);
    }

    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public EntityResolver getEntityResolver()
    {
        return entityResolver;
    }

    // TODO To remove the setters

    public void setIndexManager(IndexManager indexManager)
    {
        this.indexManager = indexManager;
    }

    public void setEntityResolver(EntityResolver entityResolver)
    {
        this.entityResolver = entityResolver;
    }

    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }
}
