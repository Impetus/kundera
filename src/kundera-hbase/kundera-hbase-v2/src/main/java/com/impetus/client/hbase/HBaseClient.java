/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
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
import com.impetus.client.hbase.admin.HBaseRow;
import com.impetus.client.hbase.query.HBaseQuery;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * @author Devender Yadav
 * 
 */
public class HBaseClient extends ClientBase implements Client<HBaseQuery>, Batcher, ClientPropertiesSetter,
        TableGenerator
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseClient.class);

    /** The handler. */
    private DataHandler handler;

    /** The reader. */
    private EntityReader reader;

    private List<Node> nodes = new ArrayList<Node>();

    private int batchSize;

    /**
     * Instantiates a new h base client.
     * 
     * @param indexManager
     *            the index manager
     * @param conf
     *            the conf
     * @param connection
     *            the connection
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
    public HBaseClient(IndexManager indexManager, Configuration conf, Connection connection, EntityReader reader,

    String persistenceUnit, Map<String, Object> externalProperties, ClientMetadata clientMetadata,

    final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.indexManager = indexManager;
        this.handler = new HBaseDataHandler(kunderaMetadata, connection);
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
        String tableName = HBaseUtils.getHTableName(entityMetadata.getSchema(), entityMetadata.getTableName());
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

            results = fetchEntity(entityClass, rowId, entityMetadata, relationNames, tableName, results, null, null,
                    null);

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
     * (non-Javadoc)
     * 
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
                            entityMetadata.getSchema(), results, null, null, null);
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
     * Find by query.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param metadata
     *            the metadata
     * @param f
     *            the f
     * @param filterClausequeue
     *            the filter clausequeue
     * @param colToOutput
     *            the col to output
     * @return the list
     */
    public <E> List<E> findByQuery(Class<E> entityClass, EntityMetadata metadata, Filter f, Queue filterClausequeue,
            List<Map<String, Object>> colToOutput)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        List<String> relationNames = entityMetadata.getRelationNames();
        String tableName = HBaseUtils.getHTableName(entityMetadata.getSchema(), entityMetadata.getTableName());
        List results = null;
        FilterList filter = null;
        if (f != null)
        {
            if (FilterList.class.isAssignableFrom(f.getClass()))
            {
                filter = (FilterList) f;
            }
            else
            {
                filter = new FilterList();
                filter.addFilter(f);
            }
        }
        if (HBaseUtils.isFindKeyOnly(metadata, colToOutput))
        {
            colToOutput = null;
            if (filter == null)
            {
                filter = new FilterList();
            }
            filter.addFilter(new KeyOnlyFilter());
        }

        try
        {
            results = fetchEntity(entityClass, null, entityMetadata, relationNames, tableName, results, filter,
                    filterClausequeue, colToOutput);
        }
        catch (IOException ioex)
        {
            log.error("Error during find by query, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }
        return results != null ? results : new ArrayList();
    }

    /**
     * Find by range.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param metadata
     *            the metadata
     * @param startRow
     *            the start row
     * @param endRow
     *            the end row
     * @param colToOutput
     *            the col to output
     * @param f
     *            the f
     * @param filterClausequeue
     *            the filter clausequeue
     * @return the list
     */
    public <E> List<E> findByRange(Class<E> entityClass, EntityMetadata metadata, byte[] startRow, byte[] endRow,
            List<Map<String, Object>> colToOutput, Filter f, Queue filterClausequeue)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        String tableName = HBaseUtils.getHTableName(entityMetadata.getSchema(), entityMetadata.getTableName());
        List results = new ArrayList();

        FilterList filter = null;
        if (f != null)
        {
            if (FilterList.class.isAssignableFrom(f.getClass()))
            {
                filter = (FilterList) f;
            }
            else
            {
                filter = new FilterList();
                filter.addFilter(f);
            }
        }
        if (HBaseUtils.isFindKeyOnly(metadata, colToOutput))
        {
            colToOutput = null;
            if (filter == null)
            {
                filter = new FilterList();
            }
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
                            subEntityMetadata, startRow, endRow, colToOutput, filter);
                    results.addAll(found);
                }
            }
            else
            {
                results = handler.readDataByRange(tableName, entityClass, metadata, startRow, endRow, colToOutput,
                        filter);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        handler.shutdown();
        externalProperties = null;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
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
     * Sets the fetch size.
     * 
     * @param fetchSize
     *            the new fetch size
     */
    public void setFetchSize(int fetchSize)
    {
        ((HBaseDataHandler) handler).setFetchSize(fetchSize);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> relations)
    {
        String tableName = HBaseUtils.getHTableName(entityMetadata.getSchema(), entityMetadata.getTableName());

        try
        {
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
        String tableName = HBaseUtils.getHTableName(joinTableData.getSchemaName(), joinTableName);
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
                    handler.writeJoinTableData(tableName, joinColumnValue, columns, joinTableName);
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
    @Override
    public void deleteByColumn(String tableName, String colFamily, String columnName, Object columnValue)
    {
        try
        {
            String hTableName = HBaseUtils.getHTableName(tableName, colFamily);
            handler.deleteRow(columnValue,columnName,colFamily,hTableName);
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
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            pKey = KunderaCoreUtils.prepareCompositeKey(m, pKey);
        }
        deleteByColumn(m.getSchema(), m.getTableName(), null, pKey);
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
        CompareOp operator = HBaseUtils.getOperator("=", false, false);

        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        String columnFamilyName = m.getTableName();

        byte[] valueInBytes = HBaseUtils.getBytes(colValue);
        SingleColumnValueFilter f = null;
        f = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes(colName), operator, valueInBytes);

        List output = new ArrayList();
        try
        {
            List<AbstractManagedType> subManagedType = getSubManagedType(entityClazz, m);
            String tableName = null;
            if (!subManagedType.isEmpty())
            {
                for (AbstractManagedType subEntity : subManagedType)
                {
                    EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            subEntity.getJavaType());
                    tableName = HBaseUtils.getHTableName(subEntityMetadata.getSchema(),
                            subEntityMetadata.getTableName());
                    List results = ((HBaseDataHandler) handler).scanData(f, tableName,
                            subEntityMetadata.getEntityClazz(), subEntityMetadata, columnFamilyName, colName);
                    if (!results.isEmpty())
                    {
                        output.addAll(results);
                    }
                }
            }
            else
            {
                tableName = HBaseUtils.getHTableName(m.getSchema(), m.getTableName());
                return ((HBaseDataHandler) handler).scanData(f, tableName, entityClazz, m, columnFamilyName, colName);
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
        CompareOp operator = HBaseUtils.getOperator("=", false, false);
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);

        byte[] valueInBytes = HBaseUtils.getBytes(columnValue);
        Filter f = new SingleColumnValueFilter(Bytes.toBytes(tableName), Bytes.toBytes(columnName), operator,
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
    @Override
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
        Map<String, List<Row>> batchData = new HashMap<String, List<Row>>();
        try
        {
            for (Node node : nodes)
            {
                if (node.isDirty())
                {
                    Row action = null;
                    node.handlePreEvent();
                    Object rowKey = node.getEntityId();
                    Object entity = node.getData();
                    EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, node.getDataClass());
                    String tableName = HBaseUtils.getHTableName(m.getSchema(), m.getTableName());
                    if (node.isInState(RemovedState.class))
                    {
                        action = handler.prepareDelete(rowKey);
                    }
                    else
                    {
                        HBaseRow hbaseRow = ((HBaseDataHandler) handler).createHbaseRow(m, entity, rowKey, null);
                        action = handler.preparePut(hbaseRow);
                    }
                    node.handlePostEvent();
                    if (!batchData.containsKey(tableName))
                    {
                        batchData.put(tableName, new ArrayList<Row>());
                    }
                    batchData.get(tableName).add(action);
                }
            }

            if (!batchData.isEmpty())
            {
                ((HBaseDataHandler) handler).batchProcess(batchData);
            }
            return batchData.size();
        }
        catch (IOException ioex)
        {
            log.error("Error while executing batch insert/update, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }

    }

    /**
     * On batch limit.
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
    public void setBatchSize(int batch_Size)
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.TableGenerator#generate(com.impetus.kundera
     * .metadata.model.TableGeneratorDiscriptor)
     */
    @Override
    public Long generate(TableGeneratorDiscriptor discriptor)
    {
        try
        {
            String tableName = HBaseUtils.getHTableName(discriptor.getSchema(), discriptor.getPkColumnValue());
            Table hTable = ((HBaseDataHandler) handler).gethTable(tableName);
            Long latestCount = hTable.incrementColumnValue(HBaseUtils.AUTO_ID_ROW.getBytes(), discriptor
                    .getPkColumnValue().getBytes(), discriptor.getValueColumnName().getBytes(), 1);
            if (latestCount == 1)
            {
                return (long) discriptor.getInitialValue();
            }
            else if (discriptor.getAllocationSize() == 1)
            {
                return latestCount + discriptor.getInitialValue();
            }
            else
            {
                return (latestCount - 1) * discriptor.getAllocationSize() + discriptor.getInitialValue();
            }
        }
        catch (IOException ioex)
        {
            log.error("Error while generating id for entity, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }
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
     * @param colToOutput
     *            the col to output
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List fetchEntity(Class entityClass, Object rowId, EntityMetadata entityMetadata,
            List<String> relationNames, String tableName, List results, FilterList filter, Queue filterClausequeue,
            List colToOutput) throws IOException
    {
        List<AbstractManagedType> subManagedType = getSubManagedType(entityClass, entityMetadata);

        if (!subManagedType.isEmpty())
        {
            for (AbstractManagedType subEntity : subManagedType)
            {
                EntityMetadata subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        subEntity.getJavaType());
                results = handler.readData(tableName, subEntityMetadata.getEntityClazz(), subEntityMetadata, rowId,
                        subEntityMetadata.getRelationNames(), filter, colToOutput);
                if (!results.isEmpty())
                {
                    break;
                }
            }
        }
        else
        {
            results = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata, rowId,
                    relationNames, filter, colToOutput);
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

}