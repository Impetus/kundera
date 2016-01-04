/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.admin.DataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler.HBaseDataWrapper;
import com.impetus.client.hbase.query.HBaseQuery;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient extends ClientBase implements Client<HBaseQuery>, Batcher, ClientPropertiesSetter
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseClient.class);

    /** The handler. */
    DataHandler handler;

    /** The reader. */
    private EntityReader reader;

    /** The nodes. */
    private List<Node> nodes = new ArrayList<Node>();

    /** The batch size. */
    private int batchSize;

    /**
     * Instantiates a new hbase client.
     * 
     * @param indexManager
     *            the index manager
     * @param conf
     *            the conf
     * @param hTablePool
     *            the h table pool
     * @param reader
     *            the reader
     * @param persistenceUnit
     *            the persistence unit
     * @param externalProperties
     *            the external properties
     * @param clientMetadata
     *            the client metadata
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public HBaseClient(IndexManager indexManager, Configuration conf, HTablePool hTablePool, EntityReader reader,

    String persistenceUnit, Map<String, Object> externalProperties, ClientMetadata clientMetadata,

    final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.indexManager = indexManager;
        this.handler = new HBaseDataHandler(kunderaMetadata, conf, hTablePool);
        this.reader = reader;
        this.clientMetadata = clientMetadata;
        getBatchSize(persistenceUnit, this.externalProperties);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object, java.util.List)
     */
    @Override
    public Object find(Class entityClass, Object rowId)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getSchema();
        Object enhancedEntity = null;
        List results = null;
        try
        {
            if (rowId == null)
            {
                return null;
            }

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
            {
                rowId = KunderaCoreUtils.prepareCompositeKey(entityMetadata, rowId);
            }

            results = fetchEntity(entityClass, rowId, entityMetadata, relationNames, tableName, results, null, null);

            if (results != null && !results.isEmpty())
            {
                enhancedEntity = results.get(0);
            }
        }
        catch (IOException e)
        {
            log.error("Error during find by id, Caused by: .", e);
            throw new KunderaException(e);
        }
        return enhancedEntity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... rowIds)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        if (rowIds == null)
        {
            return null;
        }
        List results = new ArrayList<E>();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityClass);

        List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();
        try
        {
            if (!subManagedType.isEmpty())
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            subEntity.getJavaType());
                    results = handler.readAll(subEntityMetadata.getSchema(), subEntityMetadata.getEntityClazz(),
                            subEntityMetadata, Arrays.asList(rowIds), subEntityMetadata.getRelationNames());
                    // Result will not be empty for match sub entity.
                    if (!results.isEmpty())
                    {
                        break;
                    }
                }
            }
            else
            {

                results = handler.readAll(entityMetadata.getSchema(), entityMetadata.getEntityClazz(), entityMetadata,
                        Arrays.asList(rowIds), entityMetadata.getRelationNames());
            }
        }
        catch (IOException ioex)
        {
            log.error("Error during find All , Caused by: .", ioex);
            throw new KunderaException(ioex);
        }

        return results;
    }

    /**
     * (non-Javadoc).
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param col
     *            the col
     * @return the list
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     *      java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> col)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, getPersistenceUnit(),
                entityClass);
        List<E> entities = new ArrayList<E>();
        Map<String, Field> columnFamilyNameToFieldMap = MetadataUtils.createSuperColumnsFieldMap(entityMetadata,
                kunderaMetadata);
        for (String columnFamilyName : col.keySet())
        {
            String entityId = col.get(columnFamilyName);
            if (entityId != null)
            {
                E e = null;
                try
                {
                    List results = new ArrayList();
                    fetchEntity(entityClass, entityId, entityMetadata, entityMetadata.getRelationNames(),
                            entityMetadata.getSchema(), results, null, null);
                    if (results != null)
                    {
                        e = (E) results.get(0);
                    }
                }
                catch (IOException ioex)
                {
                    log.error("Error during find for embedded entities, Caused by: .", ioex);

                    throw new KunderaException(ioex);
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
        }
        return entities;
    }

    /**
     * Method to find entities using JPQL(converted into FilterList.)
     * 
     * @param <E>
     *            parameterized entity class.
     * @param entityClass
     *            entity class.
     * @param metadata
     *            entity metadata.
     * @param f
     *            the f
     * @param filterClausequeue
     *            the filter clausequeue
     * @param columns
     *            the columns
     * @return list of entities.
     */
    public <E> List<E> findByQuery(Class<E> entityClass, EntityMetadata metadata, Filter f, Queue filterClausequeue,
            String... columns)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getSchema();
        List results = null;

        FilterList filter = new FilterList();
        if (f != null)
        {
            filter.addFilter(f);
        }
        if (isFindKeyOnly(metadata, columns))
        {
            columns = null;
            filter.addFilter(new KeyOnlyFilter());
        }

        try
        {
            results = fetchEntity(entityClass, null, entityMetadata, relationNames, tableName, results, filter,
                    filterClausequeue, columns);
        }
        catch (IOException ioex)
        {
            log.error("Error during find by query, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }
        return results != null ? results : new ArrayList();
    }

    /**
     * Handles find by range query for given start and end row key range values.
     * 
     * @param <E>
     *            parameterized entity class.
     * @param entityClass
     *            entity class.
     * @param metadata
     *            entity metadata
     * @param startRow
     *            start row.
     * @param endRow
     *            end row.
     * @param columns
     *            the columns
     * @param f
     *            the f
     * @param filterClausequeue
     *            the filter clausequeue
     * @return collection holding results.
     */
    public <E> List<E> findByRange(Class<E> entityClass, EntityMetadata metadata, byte[] startRow, byte[] endRow,
            String[] columns, Filter f, Queue filterClausequeue)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getSchema();
        List results = new ArrayList();

        FilterList filter = new FilterList();
        if (f != null)
        {
            filter.addFilter(f);
        }
        if (isFindKeyOnly(metadata, columns))
        {
            columns = null;
            filter.addFilter(new KeyOnlyFilter());
        }

        try
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    entityMetadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(entityClass);

            List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();

            if (!subManagedType.isEmpty())
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            subEntity.getJavaType());
                    List found = handler.readDataByRange(tableName, subEntityMetadata.getEntityClazz(),
                            subEntityMetadata, startRow, endRow, columns, filter);
                    results.addAll(found);
                }
            }
            else
            {
                results = handler.readDataByRange(tableName, entityClass, metadata, startRow, endRow, columns, filter);
            }
            if (showQuery && filterClausequeue.size() > 0)
            {
                KunderaCoreUtils.printQueryWithFilterClause(filterClausequeue, entityMetadata.getTableName());
            }
        }
        catch (IOException ioex)
        {
            log.error("Error during find by range, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }
        return results;
    }

    /**
     * Checks if is find key only.
     * 
     * @param metadata
     *            the metadata
     * @param columns
     *            the columns
     * @return true, if is find key only
     */
    private boolean isFindKeyOnly(EntityMetadata metadata, String[] columns)
    {
        int noOFColumnsToFind = 0;
        boolean findIdOnly = false;
        if (columns != null)
        {
            for (String column : columns)
            {
                if (column != null)
                {
                    if (column.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
                    {
                        noOFColumnsToFind++;
                        findIdOnly = true;
                    }
                    else
                    {
                        noOFColumnsToFind++;
                        findIdOnly = false;
                    }
                }
            }
        }
        if (noOFColumnsToFind == 1 && findIdOnly)
        {
            return true;
        }
        return false;
    }

    /**
     * Close handlers instance and reinstate pu properties.
     * 
     */
    @Override
    public void close()
    {
        handler.shutdown();
        externalProperties = null;
    }

    /**
     * Setter for filter.
     * 
     * @param filter
     *            filter.
     */
    public void setFilter(Filter filter)
    {
        ((HBaseDataHandler) handler).setFilter(filter);
    }

    /**
     * Adds the filter.
     * 
     * @param columnFamily
     *            the column family
     * @param filter
     *            the filter
     */
    public void addFilter(final String columnFamily, Filter filter)
    {
        ((HBaseDataHandler) handler).addFilter(columnFamily, filter);
    }

    /**
     * Reset filter.
     */
    public void resetFilter()
    {
        ((HBaseDataHandler) handler).resetFilter();
    }

    /**
     * Setter for filter.
     * 
     * @param fetchSize
     *            the new fetch size
     */
    public void setFetchSize(int fetchSize)
    {
        ((HBaseDataHandler) handler).setFetchSize(fetchSize);
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
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> relations)
    {
        String tableName = entityMetadata.getSchema();

        try
        {
            // Write data to HBase
            handler.writeData(tableName, entityMetadata, entity, id, relations, showQuery);
        }
        catch (IOException e)
        {
            throw new PersistenceException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);
            Object joinColumnValue = key;

            Map<String, Object> columns = new HashMap<String, Object>();
            for (Object childValue : values)
            {
                Object invJoinColumnValue = childValue;
                columns.put(invJoinColumnName + "_" + invJoinColumnValue, invJoinColumnValue);
            }

            if (columns != null && !columns.isEmpty())
            {
                try
                {
                    handler.createTableIfDoesNotExist(joinTableData.getSchemaName(), joinTableName);
                    handler.writeJoinTableData(joinTableData.getSchemaName(), joinColumnValue, columns, joinTableName);
                    KunderaCoreUtils.printQuery("Persist Join Table:" + joinTableName, showQuery);
                }
                catch (IOException e)
                {
                    throw new PersistenceException(e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#getForeignKeysFromJoinTable(java.lang
     * .String, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Class columnJavaType)
    {
        return handler.getForeignKeysFromJoinTable(schemaName, joinTableName, parentId, inverseJoinColumnName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        try
        {
            handler.deleteRow(columnValue, schemaName, tableName);
            KunderaCoreUtils.printQuery("Delete data from " + tableName + " for PK " + columnValue, showQuery);
        }
        catch (IOException ioex)
        {
            log.error("Error during get columns by key. Caused by: .", ioex);
            throw new PersistenceException(ioex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(metadata.getTableName());
        if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
        {
            pKey = KunderaCoreUtils.prepareCompositeKey(metadata, pKey);
        }

        for (String colTableName : secondaryTables)
        {
            deleteByColumn(metadata.getSchema(), colTableName,
                    ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), pKey);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        String columnFamilyName = m.getTableName();

        byte[] valueInBytes = HBaseUtils.getBytes(colValue);
        SingleColumnValueFilter f = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName),
                Bytes.toBytes(colName), CompareOp.EQUAL, valueInBytes);

        List output = new ArrayList();
        try
        {
            List<AbstractManagedType> subManagedType = getSubManagedType(entityClazz, m);

            if (!subManagedType.isEmpty())
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            subEntity.getJavaType());
                    List results = ((HBaseDataHandler) handler).scanData(f, subEntityMetadata.getSchema(),
                            subEntityMetadata.getEntityClazz(), subEntityMetadata, columnFamilyName, colName);
                    if (!results.isEmpty())
                    {
                        output.addAll(results);
                    }
                }
            }
            else
            {
                return ((HBaseDataHandler) handler).scanData(f, m.getSchema(), entityClazz, m, columnFamilyName,
                        colName);
            }
        }
        catch (IOException ioe)
        {
            log.error("Error during find By Relation, Caused by: .", ioe);
            throw new KunderaException(ioe);
        }
        catch (InstantiationException ie)
        {
            log.error("Error during find By Relation, Caused by: .", ie);
            throw new KunderaException(ie);
        }
        catch (IllegalAccessException iae)
        {
            log.error("Error during find By Relation, Caused by: .", iae);
            throw new KunderaException(iae);
        }

        return output;
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<HBaseQuery> getQueryImplementor()
    {
        return HBaseQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        byte[] valueInBytes = HBaseUtils.getBytes(columnValue);
        Filter f = new SingleColumnValueFilter(Bytes.toBytes(tableName), Bytes.toBytes(columnName), CompareOp.EQUAL,
                valueInBytes);
        KeyOnlyFilter keyFilter = new KeyOnlyFilter();
        FilterList filterList = new FilterList(f, keyFilter);
        try
        {
            return handler.scanRowyKeys(filterList, schemaName, tableName, columnName + "_" + columnValue, m
                    .getIdAttribute().getBindableJavaType());
        }
        catch (IOException e)
        {
            log.error("Error while executing findIdsByColumn(), Caused by: .", e);
            throw new KunderaException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera
     * .graph.Node)
     */
    public void addBatch(Node node)
    {
        if (node != null)
        {
            nodes.add(node);
        }
        onBatchLimit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        return batchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#clear()
     */
    @Override
    public void clear()
    {
        if (nodes != null)
        {
            nodes.clear();
            nodes = new ArrayList<Node>();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {
        Map<HTableInterface, List<HBaseDataWrapper>> data = new HashMap<HTableInterface, List<HBaseDataWrapper>>();

        try
        {
            for (Node node : nodes)
            {
                if (node.isDirty())
                {
                    node.handlePreEvent();
                    HTableInterface hTable = null;
                    Object rowKey = node.getEntityId();
                    Object entity = node.getData();
                    if (node.isInState(RemovedState.class))
                    {
                        delete(entity, rowKey);
                    }
                    else
                    {
                        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                node.getDataClass());

                        HBaseDataWrapper columnWrapper = new HBaseDataHandler.HBaseDataWrapper(rowKey,
                                new java.util.HashMap<String, Attribute>(), entity, metadata.getTableName());

                        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                                .getMetamodel(metadata.getPersistenceUnit());

                        EntityType entityType = metaModel.entity(node.getDataClass());

                        List<HBaseDataWrapper> embeddableData = new ArrayList<HBaseDataHandler.HBaseDataWrapper>();

                        hTable = ((HBaseDataHandler) handler).gethTable(metadata.getSchema());
                        ((HBaseDataHandler) handler).preparePersistentData(metadata.getSchema(),
                                metadata.getTableName(), entity, rowKey, metaModel, entityType.getAttributes(),
                                columnWrapper, embeddableData, showQuery);

                        List<HBaseDataWrapper> dataSet = null;
                        if (data.containsKey(hTable))
                        {
                            dataSet = data.get(metadata.getTableName());
                            addRecords(columnWrapper, embeddableData, dataSet);
                        }
                        else
                        {
                            dataSet = new ArrayList<HBaseDataHandler.HBaseDataWrapper>();
                            addRecords(columnWrapper, embeddableData, dataSet);
                            data.put(hTable, dataSet);
                        }
                    }
                    node.handlePostEvent();
                }
            }

            if (!data.isEmpty())
            {
                ((HBaseDataHandler) handler).batch_insert(data);
            }
            return data.size();
        }
        catch (IOException ioex)
        {
            log.error("Error while executing batch insert/update, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }

    }

    /**
     * Add records to data wrapper.
     * 
     * @param columnWrapper
     *            column wrapper
     * @param embeddableData
     *            embeddable data
     * @param dataSet
     *            data collection set
     */
    private void addRecords(HBaseDataWrapper columnWrapper, List<HBaseDataWrapper> embeddableData,
            List<HBaseDataWrapper> dataSet)
    {
        dataSet.add(columnWrapper);

        if (!embeddableData.isEmpty())
        {
            dataSet.addAll(embeddableData);
        }
    }

    /**
     * Check on batch limit.
     */
    private void onBatchLimit()
    {
        if (batchSize > 0 && batchSize == nodes.size())
        {
            executeBatch();
            nodes.clear();
        }
    }

    /**
     * Gets the batch size.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     *            the pu properties
     * @return the batch size
     */
    private void getBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_Size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                : null;
        if (batch_Size != null)
        {
            setBatchSize(Integer.valueOf(batch_Size));
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                    persistenceUnit);
            setBatchSize(puMetadata.getBatchSize());
        }
    }

    /**
     * Sets the batch size.
     * 
     * @param batch_Size
     *            the new batch size
     */
    void setBatchSize(int batch_Size)
    {
        this.batchSize = batch_Size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        new HBaseClientProperties().populateClientProperties(client, properties);
    }

    /**
     * Reset.
     */
    public void reset()
    {
        ((HBaseDataHandler) handler).reset();
    }

    /**
     * Next.
     * 
     * @param m
     *            the m
     * @return the object
     */
    public Object next(EntityMetadata m)
    {
        return ((HBaseDataHandler) handler).next(m);
    }

    /**
     * Checks for next.
     * 
     * @return true, if successful
     */
    public boolean hasNext()
    {
        return ((HBaseDataHandler) handler).hasNext();
    }

    /**
     * Gets the handle.
     * 
     * @return the handle
     */
    public HBaseDataHandler getHandle()
    {
        return ((HBaseDataHandler) handler).getHandle();
    }

    /**
     * Fetch entity.
     * 
     * @param entityClass
     *            the entity class
     * @param rowId
     *            the row id
     * @param entityMetadata
     *            the entity metadata
     * @param relationNames
     *            the relation names
     * @param tableName
     *            the table name
     * @param results
     *            the results
     * @param filter
     *            the filter
     * @param filterClausequeue
     *            the filter clausequeue
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List fetchEntity(Class entityClass, Object rowId, EntityMetadata entityMetadata,
            List<String> relationNames, String tableName, List results, FilterList filter, Queue filterClausequeue,
            String... columns) throws IOException
    {
        results = new ArrayList();
        List<AbstractManagedType> subManagedType = getSubManagedType(entityClass, entityMetadata);

        if (!subManagedType.isEmpty())
        {
            for (AbstractManagedType subEntity : subManagedType)
            {
                EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        subEntity.getJavaType());
                List tempResults = handler.readData(tableName, subEntityMetadata.getEntityClazz(), subEntityMetadata,
                        rowId, subEntityMetadata.getRelationNames(), filter, columns);
                if (tempResults != null && !tempResults.isEmpty())
                {
                    results.addAll(tempResults);
                }
            }
        }
        else
        {
            results = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata, rowId,
                    relationNames, filter, columns);
        }
        if (rowId != null)
        {
            KunderaCoreUtils.printQuery("Fetch data from " + entityMetadata.getTableName() + " for PK " + rowId,
                    showQuery);
        }
        else if (showQuery && filterClausequeue.size() > 0)
        {
            KunderaCoreUtils.printQueryWithFilterClause(filterClausequeue, entityMetadata.getTableName());
        }

        return results;
    }

    /**
     * Gets the sub managed type.
     * 
     * @param entityClass
     *            the entity class
     * @param entityMetadata
     *            the entity metadata
     * @return the sub managed type
     */
    private List<AbstractManagedType> getSubManagedType(Class entityClass, EntityMetadata entityMetadata)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EntityType entityType = metaModel.entity(entityClass);

        List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();
        return subManagedType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(HBaseIdGenerator.class);
    }

}