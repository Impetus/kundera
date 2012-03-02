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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.impetus.client.hbase.admin.DataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient implements com.impetus.kundera.client.Client
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseClient.class);

    /** The handler. */
    private DataHandler handler;

    /** The index manager. */
    private IndexManager indexManager;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new h base client.
     * 
     * @param indexManager
     *            the index manager
     * @param conf
     *            the conf
     * @param hTablePool
     *            the h table pool
     * @param reader
     *            the reader
     */
    public HBaseClient(IndexManager indexManager, HBaseConfiguration conf, HTablePool hTablePool, EntityReader reader)
    {
        this.indexManager = indexManager;
        this.handler = new HBaseDataHandler(conf, hTablePool);
        this.reader = reader;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object, java.util.List)
     */
    @Override
    public <E> E find(Class<E> entityClass, Object rowId, List<String> relationNames)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getTableName();
        Object enhancedEntity = null;
        try
        {
            enhancedEntity = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata,
                    rowId != null ? rowId.toString() : null, relationNames);
        }
        catch (IOException e)
        {
            throw new KunderaException(e);
        }
        return (E) enhancedEntity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... rowIds) 
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        for (Object rowKey : rowIds)
        {
            E e = null;
            try
            {
                e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                        rowKey.toString(), null);
            }
            catch (IOException e1)
            {
                throw new KunderaException(e1);
            }
            entities.add(e);
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> col) 
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        Map<String, Field> columnFamilyNameToFieldMap = MetadataUtils.createSuperColumnsFieldMap(entityMetadata);
        for (String columnFamilyName : col.keySet())
        {
            String entityId = col.get(columnFamilyName);
            E e = null;
            try
            {
                e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                        entityId, null);
            }
            catch (IOException e1)
            {
                throw new KunderaException(e1);
            }

            Field columnFamilyField = columnFamilyNameToFieldMap.get(columnFamilyName.substring(0,
                    columnFamilyName.indexOf("|")));
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
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        handler.shutdown();

    }

    /*
     * @Override public void delete(EnhancedEntity enhancedEntity) throws
     * Exception { throw new RuntimeException("TODO:not yet supported");
     * 
     * }
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIndexManager()
     */
    @Override
    public final IndexManager getIndexManager()
    {
        return indexManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#setPersistenceUnit(java.lang.String)
     */
    @Override
    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persist(com.impetus.kundera.persistence
     * .handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata entityMetadata)
    {
        Object entity = entityGraph.getParentEntity();
        String id = entityGraph.getParentId();
        onPersist(entityMetadata, entity, id,
                RelationHolder.addRelation(entityGraph, entityGraph.getRevFKeyName(), entityGraph.getRevFKeyValue()));

        if (entityGraph.getRevParentClass() != null)
        {
            getIndexManager().write(entityMetadata, entity, entityGraph.getRevFKeyValue(),
                    entityGraph.getRevParentClass());
        }
        else
        {
            getIndexManager().write(entityMetadata, entityGraph.getParentEntity());
        }

        return null;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata entityMetadata)
    {
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();
        onPersist(entityMetadata, childEntity, id, RelationHolder.addRelation(entitySaveGraph, rlName, rlValue));
        onIndex(childEntity, entitySaveGraph, entityMetadata, rlValue);
    }

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param relations
     *            the relations
     */
    private void onPersist(EntityMetadata entityMetadata, Object entity, String id, List<RelationHolder> relations)
    {
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

        // Check whether this table exists, if not create it
        columnFamilyNames.addAll(entityMetadata.getEmbeddedColumnFieldNames());

        // Add relationship fields if they are there
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                columnFamilyNames.add(rh.getRelationName());
            }
        }

        try
        {
            handler.createTableIfDoesNotExist(tableName, columnFamilyNames.toArray(new String[0]));

            // Write data to HBase

            handler.writeData(tableName, entityMetadata, entity, id, relations);
        }
        catch (IOException e)
        {
            throw new PersistenceException(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persistJoinTable(java.lang.String,
     * java.lang.String, java.lang.String,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,

    EntityMetadata relMetadata, Object primaryKey, Object childEntity)

    {
        String parentId = (String) primaryKey;
        Map<String, String> columns = new HashMap<String, String>();
        try
        {
            if (Collection.class.isAssignableFrom(childEntity.getClass()))
            {
                Collection children = (Collection) childEntity;

                for (Object child : children)
                {
                    String childId = PropertyAccessorHelper.getId(child, relMetadata);
                    columns.put(inverseJoinColumnName + "_" + childId, childId);
                }

            }
            else
            {
                String childId = PropertyAccessorHelper.getId(childEntity, relMetadata);
                columns.put(inverseJoinColumnName + "_" + childId, childId);
            }

            if (columns != null && !columns.isEmpty())
            {
                handler.createTableIfDoesNotExist(joinTableName, Constants.JOIN_COLUMNS_FAMILY_NAME);
                handler.writeJoinTableData(joinTableName, parentId, columns);
            }
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName,
            String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String parentId = objectGraph.getParentId();
        return handler.getForeignKeysFromJoinTable(joinTableName, parentId, inverseJoinColumnName);

    }

    @Override
    public <E> List<E> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId)
    {
        return handler.findParentEntityFromJoinTable(parentMetadata, joinTableName, joinColumnName,
                inverseJoinColumnName, childId);

    }

    @Override
    public void deleteFromJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String pKey = objectGraph.getParentId();
        try
        {
            handler.deleteRow(pKey.toString(), joinTableName);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * On index.
     * 
     * @param childEntity
     *            the child entity
     * @param entitySaveGraph
     *            the entity save graph
     * @param metadata
     *            the metadata
     * @param rlValue
     *            the rl value
     */
    private void onIndex(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata, String rlValue)
    {
        if (!entitySaveGraph.isSharedPrimaryKey())
        {
            getIndexManager().write(metadata, childEntity, rlValue, entitySaveGraph.getParentEntity().getClass());
        }
        else
        {
            getIndexManager().write(metadata, childEntity);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata entityMetadata, Object rowId, List<String> relationNames)
    {
        String tableName = entityMetadata.getTableName();
        Object enhancedEntity = null;
        try
        {
            enhancedEntity = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata,
                    rowId != null ? rowId.toString() : null, relationNames);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return enhancedEntity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata metadata) 
    {
        try
        {
            handler.deleteRow(pKey.toString(), metadata.getTableName());
        }
        catch (IOException e)
        {
            throw new KunderaException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    public List<Object> find(String colName, String colValue, EntityMetadata m)
    {
        throw new UnsupportedOperationException("Method not supported");
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

}
