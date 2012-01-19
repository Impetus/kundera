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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
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

    private EntityReader reader;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The timestamp. */
    private long timestamp;

    /**
     * default constructor.
     * 
     * @param indexManager
     *            the index manager
     */
    public PelopsClient(IndexManager indexManager, EntityReader reader)
    {
        this.indexManager = indexManager;
        this.dataHandler = new PelopsDataHandler(this);
        this.reader = reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(com.impetus.kundera.proxy.
     * EnhancedEntity)
     */
    @Override
    public void persist(EnhancedEntity enhancedEntity) throws Exception
    {
        // DELETE it.
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String)
     */
    @Override
    @Deprecated
    public final <E> E find(Class<E> entityClass, String rowId, List<String> relationNames) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        return (E) find(entityClass, entityMetadata, rowId, relationNames);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    public final Object find(Class<?> clazz, EntityMetadata metadata, String rowId, List<String> relationNames)
    {

        List<Object> result = null;
        try
        {
            result = (List<Object>) find(clazz, relationNames, relationNames != null, metadata, rowId);
        }
        catch (Exception e)
        {
            log.error("Error on retrieval" + e.getMessage());
            throw new PersistenceException(e.getMessage());
        }

        return result != null & !result.isEmpty() ? result.get(0) : null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String[])
     */
    @Override
    public final <E> List<E> find(Class<E> entityClass, String... rowIds) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> results = new ArrayList<E>();
        for (String rowKey : rowIds)
        {
            List<E> r = (List<E>) find(entityClass, entityMetadata, rowKey, null);
            if (r != null)
            {
                results.addAll(r);
            }
        }

        return results.isEmpty() ? null : results;
    }

    /**
     * Method to return list of entities for given below attributes:
     * 
     * @param <E>
     *            entity to be returned
     * @param entityClass
     *            entity class
     * @param relationNames
     *            relation names
     * @param isWrapReq
     *            true, in case it needs to populate enhance entity.
     * @param metadata
     *            entity metadata.
     * @param rowIds
     *            array of row key s
     * @return list of wrapped entities.
     * @throws Exception
     *             throws exception. don't know why TODO: why is it throwing
     *             exception. need to take care as part of exception handling
     *             exercise.
     */
    public final List find(Class entityClass, List<String> relationNames, boolean isWrapReq, EntityMetadata metadata,
            String... rowIds) throws Exception
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        PelopsDataHandler handler = new PelopsDataHandler(this);

        List entities = handler.fromThriftRow(selector, entityClass, metadata, relationNames, isWrapReq, rowIds);

        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
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
                    entityMetadata.getTableName(), entityId,
                    new String[] { superColumnName.substring(0, superColumnName.indexOf("|")) });
            E e = (E) dataHandler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                    new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
            entities.add(e);
        }
        return entities;
    }

    /**
     * Load super columns.
     * 
     * @param keyspace
     *            the keyspace
     * @param columnFamily
     *            the column family
     * @param rowId
     *            the row id
     * @param superColumnNames
     *            the super column names
     * @return the list
     * @throws Exception
     *             the exception
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

    // /* (non-Javadoc)
    // * @see
    // com.impetus.kundera.client.Client#delete(com.impetus.kundera.proxy.EnhancedEntity)
    // */
    // // @Override
    // public final void delete(EnhancedEntity enhancedEntity) throws Exception
    // {
    //
    // if (!isOpen())
    // {
    // throw new PersistenceException("PelopsClient is closed.");
    // }
    //
    // EntityMetadata entityMetadata =
    // KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(),
    // enhancedEntity
    // .getEntity().getClass());
    //
    // RowDeletor rowDeletor =
    // Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
    // rowDeletor.deleteRow(entityMetadata.getTableName(),
    // enhancedEntity.getId(), ConsistencyLevel.ONE);
    // getIndexManager().remove(entityMetadata, enhancedEntity.getEntity(),
    // enhancedEntity.getId());
    // }
    //

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata metadata) throws Exception
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
        rowDeletor.deleteRow(metadata.getTableName(), pKey.toString(), ConsistencyLevel.ONE);
        getIndexManager().remove(metadata, entity, pKey.toString());
    }

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

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    private final boolean isOpen()
    {
        return !closed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public final void close()
    {
        this.indexManager.flush();
        this.dataHandler = null;
        closed = true;

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
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object)
     */
    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata metadata)
    {
        try
        {
            Object entity = entityGraph.getParentEntity();
            String id = entityGraph.getParentId();
            PelopsDataHandler.ThriftRow tf = populateTfRow(entity, id, metadata);

            if (entityGraph.getRevFKeyName() != null)
            {
                addRelation(entityGraph, metadata, entityGraph.getRevFKeyName(), entityGraph.getRevFKeyValue(), tf);
            }

            onPersist(metadata, entity, tf);

            if (entityGraph.getRevParentClass() != null)
            {
                getIndexManager().write(metadata, entity, entityGraph.getRevFKeyValue(),
                        entityGraph.getRevParentClass());
            }
            else
            {
                getIndexManager().write(metadata, entity);
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
        return null;
    }

    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata)
    {
        // you got child entity and
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();
        try
        {
            PelopsDataHandler.ThriftRow tf = populateTfRow(childEntity, id, metadata);
            if (rlName != null)
            {
                addRelation(entitySaveGraph, metadata, rlName, rlValue, tf);
            }

            onPersist(metadata, childEntity, tf);
            onIndex(childEntity, entitySaveGraph, metadata, rlValue);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            new PersistenceException(e.getMessage());
        }

    }

    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {

        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));

        String parentId = objectGraph.getParentId();
        List<Column> columns = new ArrayList<Column>();

        if (Collection.class.isAssignableFrom(objectGraph.getChildEntity().getClass()))
        {
            Collection children = (Collection) objectGraph.getChildEntity();

            for (Object child : children)
            {

                addColumnsToJoinTable(inverseJoinColumnName, relMetadata, columns, child);
            }

        }
        else
        {
            Object child = objectGraph.getChildEntity();
            addColumnsToJoinTable(inverseJoinColumnName, relMetadata, columns, child);
        }

        mutator.writeColumns(joinTableName, new Bytes(parentId.getBytes()),
                Arrays.asList(columns.toArray(new Column[0])));
        mutator.execute(ConsistencyLevel.ONE);
    }

    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName,
            String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String parentId = objectGraph.getParentId();
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
        List<Column> columns = selector.getColumnsFromRow(joinTableName, new Bytes(parentId.getBytes()),
                Selector.newColumnsPredicateAll(true, 10), ConsistencyLevel.ONE);

        PelopsDataHandler handler = new PelopsDataHandler(this);
        List<E> foreignKeys = handler.getForeignKeysFromJoinTable(inverseJoinColumnName, columns);
        return foreignKeys;
    }

    /**
     * @param inverseJoinColumnName
     * @param relMetadata
     * @param columns
     * @param child
     * @throws PropertyAccessException
     */
    private void addColumnsToJoinTable(String inverseJoinColumnName, EntityMetadata relMetadata, List<Column> columns,
            Object child)
    {
        String childId = null;
        try
        {
            childId = PropertyAccessorHelper.getId(child, relMetadata);
            Column col = new Column();
            col.setName(PropertyAccessorFactory.STRING.toBytes(inverseJoinColumnName + "_" + childId));
            col.setValue(PropertyAccessorFactory.STRING.toBytes(childId));
            col.setTimestamp(System.currentTimeMillis());
            columns.add(col);
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * Find.
     * 
     * @param ixClause
     *            the ix clause
     * @param m
     *            the m
     * @return the list
     */
    public List find(List<IndexClause> ixClause, EntityMetadata m, boolean isRelation, List<String> relations)
    {
        // ixClause can be 0,1 or more!
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        List<Object> entities = null;
        if (ixClause.isEmpty())
        {
            Map<Bytes, List<Column>> qResults = selector.getColumnsFromRows(m.getTableName(), new KeyRange(
                    Integer.MAX_VALUE), slicePredicate, ConsistencyLevel.ONE);
            entities = new ArrayList<Object>(qResults.size());
            populateData(m, qResults, entities, isRelation, relations);
        }

        for (IndexClause ix : ixClause)
        {
            Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate,
                    ConsistencyLevel.ONE);
            entities = new ArrayList<Object>(qResults.size());
            // iterate through complete map and
            populateData(m, qResults, entities, isRelation, relations);
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
     */
    private void populateData(EntityMetadata m, Map<Bytes, List<Column>> qResults, List<Object> entities,
            boolean isRelational, List<String> relationNames)
    {
        Iterator<Bytes> rowIter = qResults.keySet().iterator();
        while (rowIter.hasNext())
        {
            Bytes rowKey = rowIter.next();
            List<Column> columns = qResults.get(rowKey);
            try
            {
                Object e = dataHandler.fromColumnThriftRow(m.getEntityClazz(), m,
                        dataHandler.new ThriftRow(Bytes.toUTF8(rowKey.toByteArray()), m.getTableName(), columns, null),
                        relationNames, isRelational);
                entities.add(e);
            }
            catch (IllegalStateException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
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
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        SlicePredicate slicePredicate = Selector.newColumnsPredicateAll(false, Integer.MAX_VALUE);
        List<Object> entities = null;
        IndexClause ix = Selector.newIndexClause(Bytes.EMPTY, Integer.MAX_VALUE,
                Selector.newIndexExpression(colName, IndexOperator.EQ, Bytes.fromByteArray(colValue.getBytes())));
        Map<Bytes, List<Column>> qResults = selector.getIndexedColumns(m.getTableName(), ix, slicePredicate,
                ConsistencyLevel.ONE);
        entities = new ArrayList<Object>(qResults.size());
        // iterate through complete map and
        populateData(m, qResults, entities, false, null);

        return entities;
    }

    public List<EnhanceEntity> find(EntityMetadata m, List<String> relationNames, List<IndexClause> conditions)
    {

        return (List<EnhanceEntity>) find(conditions, m, true, relationNames);
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

    /**
     * Adds the relation.
     * 
     * @param entitySaveGraph
     *            the entity save graph
     * @param rlName
     *            the rl name
     * @param rlValue
     *            the rl value
     * @param tf
     *            the tf
     * @throws PropertyAccessException
     *             the property access exception
     */
    private void addRelation(EntitySaveGraph entitySaveGraph, EntityMetadata m, String rlName, String rlValue,
            PelopsDataHandler.ThriftRow tf) throws PropertyAccessException
    {
        if (!entitySaveGraph.isSharedPrimaryKey())
        {
            if (m.getEmbeddedColumnsAsList().isEmpty())
            {
                Column col = populateFkey(rlName, rlValue, timestamp);
                tf.addColumn(col);
            }
            else
            {
                SuperColumn superColumn = new SuperColumn();
                superColumn.setName(rlName.getBytes());
                Column column = populateFkey(rlName, rlValue, timestamp);
                superColumn.addToColumns(column);
                tf.addSuperColumn(superColumn);
            }

        }
    }

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
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param metadata
     *            the metadata
     * @return the pelops data handler n. thrift row
     * @throws Exception
     *             the exception
     */
    private PelopsDataHandler.ThriftRow populateTfRow(Object entity, String id, EntityMetadata metadata)
            throws Exception
    {

        String columnFamily = metadata.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        PelopsDataHandler handler = new PelopsDataHandler(this);
        PelopsDataHandler.ThriftRow tf = handler.toThriftRow(this, entity, id, metadata, columnFamily);
        timestamp = handler.getTimestamp();
        return tf;
    }

    /**
     * On persist.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param tf
     *            the tf
     */
    private void onPersist(EntityMetadata metadata, Object entity, PelopsDataHandler.ThriftRow tf)
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
                mutator.writeSubColumns(metadata.getTableName(), tf.getId(), Bytes.toUTF8(sc.getName()),
                        sc.getColumns());

            }

        }

        mutator.execute(ConsistencyLevel.ONE);
        tf = null;
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