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

package com.impetus.client.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.proxy.EnhancedEntity;


/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements Client
{

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private PelopsDataHandler dataHandler;

    /** The index manager. */
    private IndexManager indexManager;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The timestamp. */
    private long timestamp;

    /**
     * default constructor.
     *
     * @param indexManager the index manager
     */
    public PelopsClient(IndexManager indexManager)
    {
        this.indexManager = indexManager;
        this.dataHandler = new PelopsDataHandler(this);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#persist(com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public void persist(EnhancedEntity enhancedEntity) throws Exception
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity
                .getEntity().getClass());
        String keyspace = entityMetadata.getSchema();
        String columnFamily = entityMetadata.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        PelopsDataHandler.ThriftRow tf = dataHandler.toThriftRow(this, enhancedEntity, entityMetadata, columnFamily);

        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));

        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if (thriftColumns != null && !thriftColumns.isEmpty())
        {
            mutator.writeColumns(columnFamily, new Bytes(tf.getId().getBytes()),
                    Arrays.asList(tf.getColumns().toArray(new Column[0])));
        }

        if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
        {
            for (SuperColumn sc : thriftSuperColumns)
            {
                Bytes.toUTF8(sc.getColumns().get(0).getValue());
                mutator.writeSubColumns(columnFamily, tf.getId(), Bytes.toUTF8(sc.getName()), sc.getColumns());

            }

        }
        mutator.execute(ConsistencyLevel.ONE);

        getIndexManager().write(entityMetadata, enhancedEntity.getEntity());

        tf = null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.String)
     */
    @Override
    public final <E> E find(Class<E> entityClass, String rowId) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        E e = (E) dataHandler.fromThriftRow(selector, entityClass, entityMetadata, rowId.toString());

        return e;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    public final Object find(Class<?> clazz, EntityMetadata metadata, String rowId)
    {

        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        Object entity = null;
        PelopsDataHandlerN handler = new PelopsDataHandlerN(this);
        try
        {
            entity = handler.fromThriftRow(selector, clazz, metadata, rowId.toString());
        }
        catch (Exception e)
        {
            throw new PersistenceException(e.getMessage());
        }

        return entity;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.String[])
     */
    @Override
    public final <E> List<E> find(Class<E> entityClass, String... rowIds) throws Exception
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        PelopsDataHandlerN handler = new PelopsDataHandlerN(this);

        List<E> entities = (List<E>) handler.fromThriftRow(selector, entityClass, entityMetadata, rowIds);

        return entities;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> superColumnMap) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        for (String superColumnName : superColumnMap.keySet())
        {
            String entityId = superColumnMap.get(superColumnName);
            List<SuperColumn> superColumnList = loadSuperColumns(entityMetadata.getSchema(),
                    entityMetadata.getTableName(), entityId, new String[] { superColumnName });
            E e = (E) dataHandler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                    new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
            entities.add(e);
        }
        return entities;
    }

    /**
     * Load super columns.
     *
     * @param keyspace the keyspace
     * @param columnFamily the column family
     * @param rowId the row id
     * @param superColumnNames the super column names
     * @return the list
     * @throws Exception the exception
     */
    private final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames) throws Exception
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                ConsistencyLevel.ONE);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#loadData(javax.persistence.Query)
     */
    @Override
    public <E> List<E> loadData(Query query) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#delete(com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public final void delete(EnhancedEntity enhancedEntity) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity
                .getEntity().getClass());

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
        rowDeletor.deleteRow(entityMetadata.getTableName(), enhancedEntity.getId(), ConsistencyLevel.ONE);
        getIndexManager().remove(entityMetadata, enhancedEntity.getEntity(), enhancedEntity.getId());
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getIndexManager()
     */
    @Override
    public final IndexManager getIndexManager()
    {
        return indexManager;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
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

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public final void close()
    {
        this.indexManager.flush();
        this.dataHandler = null;
        closed = true;

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#setPersistenceUnit(java.lang.String)
     */
    @Override
    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object)
     */
    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata metadata)
    {
        try
        {
            Object entity = entityGraph.getParentEntity();
            String id = entityGraph.getParentId();
            PelopsDataHandlerN.ThriftRow tf = populateTfRow(entity, id, metadata);
            onPersist(metadata, entity, tf);
            getIndexManager().write(metadata, entity);

        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata, boolean)
     */
    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata)
    {
        // you got child entity and
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();
        try
        {
            PelopsDataHandlerN.ThriftRow tf = populateTfRow(childEntity, id, metadata);
            addRelation(entitySaveGraph, rlName, rlValue, tf);
            onPersist(metadata, childEntity, tf);
            onIndex(childEntity, entitySaveGraph, metadata, rlValue);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            new PersistenceException(e.getMessage());
        }

    }

    /**
     * On index.
     *
     * @param childEntity the child entity
     * @param entitySaveGraph the entity save graph
     * @param metadata the metadata
     * @param rlValue the rl value
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

    /**
     * Adds the relation.
     *
     * @param entitySaveGraph the entity save graph
     * @param rlName the rl name
     * @param rlValue the rl value
     * @param tf the tf
     * @throws PropertyAccessException the property access exception
     */
    private void addRelation(EntitySaveGraph entitySaveGraph, String rlName, String rlValue,
            PelopsDataHandlerN.ThriftRow tf) throws PropertyAccessException
    {
        if (!entitySaveGraph.isSharedPrimaryKey())
        {
            Column col = populateFkey(rlName, rlValue, timestamp);
            tf.addColumn(col);
        }
    }

    /**
     * Populates foreign key as column.
     *
     * @param rlName relation name
     * @param rlValue relation value
     * @param timestamp the timestamp
     * @return the column
     * @throws PropertyAccessException the property access exception
     */
    private Column populateFkey(String rlName, String rlValue, long timestamp) throws PropertyAccessException
    {
        Column col = new Column();
        col.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        col.setValue(rlValue.getBytes());
        col.setTimestamp(timestamp);
        return col;
    }

    /**
     * Populate tf row.
     *
     * @param entity the entity
     * @param id the id
     * @param metadata the metadata
     * @return the pelops data handler n. thrift row
     * @throws Exception the exception
     */
    private PelopsDataHandlerN.ThriftRow populateTfRow(Object entity, String id, EntityMetadata metadata)
            throws Exception
    {

        String columnFamily = metadata.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        PelopsDataHandlerN handler = new PelopsDataHandlerN(this);
        PelopsDataHandlerN.ThriftRow tf = handler.toThriftRow(this, entity, id, metadata, columnFamily);
        timestamp = handler.getTimestamp();
        return tf;
    }

    /**
     * On persist.
     *
     * @param metadata the metadata
     * @param entity the entity
     * @param tf the tf
     */
    private void onPersist(EntityMetadata metadata, Object entity, PelopsDataHandlerN.ThriftRow tf)
    {
        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));

        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if (thriftColumns != null && !thriftColumns.isEmpty())
        {
            mutator.writeColumns(metadata.getTableName(), new Bytes(tf.getId().getBytes()),
                    Arrays.asList(tf.getColumns().toArray(new Column[0])));
        }

        if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
        {
            for (SuperColumn sc : thriftSuperColumns)
            {
                System.out.println(Bytes.toUTF8(sc.getColumns().get(0).getValue()));
                mutator.writeSubColumns(metadata.getTableName(), tf.getId(), Bytes.toUTF8(sc.getName()),
                        sc.getColumns());

            }

        }

        mutator.execute(ConsistencyLevel.ONE);
        tf = null;
    }
}
