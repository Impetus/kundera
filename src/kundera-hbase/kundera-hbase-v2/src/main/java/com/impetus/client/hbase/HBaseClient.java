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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
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
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class HBaseClient.
 * 
 * @author Devender Yadav
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
        this.batchSize = getBatchSize(persistenceUnit, this.externalProperties);
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
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        Object enhancedEntity = null;
        if (rowId == null)
        {
            return null;
        }
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            rowId = KunderaCoreUtils.prepareCompositeKey(m, rowId);
        }
        try
        {
            List results = findData(m, rowId, null, null, null, null);
            if (results != null && !results.isEmpty())
            {
                enhancedEntity = results.get(0);
            }
        }
        catch (Exception e)
        {
            log.error("Error during find by id, Caused by: .", e);
            throw new KunderaException("Error during find by id, Caused by: .", e);
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
        List<E> results = new ArrayList<E>();

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
            throw new KunderaException("Error during find All , Caused by: .", ioex);
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
     * @param column
     *            the column
     * @return the list
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     *      java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> column)
    {
        throw new UnsupportedOperationException("This feature is not supported in HBase");
    }

    /**
     * Find data.
     * 
     * @param <E>
     *            the element type
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param startRow
     *            the start row
     * @param endRow
     *            the end row
     * @param columnsToOutput
     *            the columns to output
     * @param filters
     *            the filters
     * @return the list
     */
    public <E> List<E> findData(EntityMetadata m, Object rowKey, byte[] startRow, byte[] endRow,
            List<Map<String, Object>> columnsToOutput, Filter filters)
    {
        String tableName = HBaseUtils.getHTableName(m.getSchema(), m.getTableName());
        FilterList filterList = getFilterList(filters);
        try
        {
            return handler.readData(tableName, m, rowKey, startRow, endRow, columnsToOutput, filterList);
        }
        catch (IOException ioex)
        {
            log.error("Error during find by range, Caused by: .", ioex);
            throw new KunderaException("Error during find by range, Caused by: .", ioex);
        }
    }

    /**
     * Gets the filter list.
     * 
     * @param filters
     *            the filters
     * @return the filter list
     */
    private FilterList getFilterList(Filter filters)
    {
        FilterList filterList = null;
        if (filters != null)
        {
            if (FilterList.class.isAssignableFrom(filters.getClass()))
            {
                filterList = (FilterList) filters;
            }
            else
            {
                filterList = new FilterList();
                filterList.addFilter(filters);
            }
        }
        return filterList;
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
                columns.put(invJoinColumnName + "_" + childValue, childValue);
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
            handler.deleteRow(columnValue, columnName, colFamily, hTableName);
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
    public void delete(Object entity, Object rowKey)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            rowKey = KunderaCoreUtils.prepareCompositeKey(m, rowKey);
        }
        deleteByColumn(m.getSchema(), m.getTableName(), null, rowKey);
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
        String tableName = HBaseUtils.getHTableName(m.getSchema(), m.getTableName());
        String columnFamilyName = m.getTableName();
        byte[] valueInBytes = HBaseUtils.getBytes(colValue);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName),
                Bytes.toBytes(colName), CompareOp.EQUAL, valueInBytes);
        filter.setFilterIfMissing(true);
        try
        {
            return ((HBaseDataHandler) handler).readData(tableName, m, null, null, null, null, getFilterList(filter));
        }
        catch (IOException ex)
        {
            log.error("Error during find By Relation, Caused by: .", ex);
            throw new KunderaException("Error during find By Relation, Caused by: .", ex);
        }
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
        CompareOp operator = HBaseUtils.getOperator("=", false, false).getOperator();
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
    private int getBatchSize(String persistenceUnit, Map<String, Object> puProperties)
    {
        String batch_size = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_BATCH_SIZE)
                : null;
        return batch_size != null ? Integer.valueOf(batch_size) : KunderaMetadataManager.getPersistenceUnitMetadata(
                kunderaMetadata, persistenceUnit).getBatchSize();
    }

    /**
     * Sets the batch size.
     * 
     * @param batch_size
     *            the new batch size
     */
    public void setBatchSize(int batch_size)
    {
        this.batchSize = batch_size;
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
     * @param columnsToOutput
     *            the columns to output
     * @return the object
     */
    public Object next(EntityMetadata m, List<Map<String, Object>> columnsToOutput)
    {
        return ((HBaseDataHandler) handler).next(m, columnsToOutput);
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