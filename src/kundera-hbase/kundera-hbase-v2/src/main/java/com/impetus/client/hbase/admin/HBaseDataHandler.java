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
package com.impetus.client.hbase.admin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseDataWrapper;
import com.impetus.client.hbase.Reader;
import com.impetus.client.hbase.Writer;
import com.impetus.client.hbase.service.HBaseReader;
import com.impetus.client.hbase.service.HBaseWriter;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class HBaseDataHandler.
 * 
 * @author Pragalbh Garg
 */
public class HBaseDataHandler implements DataHandler
{
    /** the log used by this class. */
    private static Logger logger = LoggerFactory.getLogger(HBaseDataHandler.class);

    /** The admin. */
    private HBaseAdmin admin;

    /** The connection. */
    private Connection connection;

    /** The hbase reader. */
    private Reader hbaseReader = new HBaseReader();

    /** The hbase writer. */
    private Writer hbaseWriter = new HBaseWriter();

    /** The filter. */
    private FilterList filter = null;

    /** The filters. */
    private Map<String, FilterList> filters = new ConcurrentHashMap<String, FilterList>();

    /** The kundera metadata. */
    private KunderaMetadata kunderaMetadata;

    /**
     * Instantiates a new hBase data handler.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     * @param connection
     *            the connection
     */
    public HBaseDataHandler(final KunderaMetadata kunderaMetadata, final Connection connection)
    {
        this.kunderaMetadata = kunderaMetadata;
        this.connection = connection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#createTableIfDoesNotExist(
     * java.lang.String, java.lang.String[])
     */
    @Override
    public void createTableIfDoesNotExist(final String tableName, final String... colFamily)
            throws MasterNotRunningException, IOException
    {
        if (!admin.tableExists(Bytes.toBytes(tableName)))
        {
            HTableDescriptor htDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String columnFamily : colFamily)
            {
                HColumnDescriptor familyMetadata = new HColumnDescriptor(columnFamily);
                htDescriptor.addFamily(familyMetadata);
            }
            admin.createTable(htDescriptor);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#readData(java.lang.String,
     * java.lang.Class, com.impetus.kundera.metadata.model.EntityMetadata,
     * java.lang.String, java.util.List)
     */
    @Override
    public List readAll(final String tableName, Class clazz, EntityMetadata m, final List<Object> rowKey,
            List<String> relationNames, String... columns) throws IOException
    {
        Table hTable = gethTable(tableName);
        List<HBaseDataWrapper> results = ((HBaseReader) hbaseReader).loadAll(hTable, rowKey, null, columns);
        return onRead(m, null, hTable, results);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#readData(java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * byte[], byte[], java.util.List,
     * org.apache.hadoop.hbase.filter.FilterList)
     */
    @Override
    public List readData(String tableName, EntityMetadata m, final Object rowKey, byte[] startRow, byte[] endRow,
            List<Map<String, Object>> columnsToOutput, FilterList filterList) throws IOException
    {
        Table hTable = gethTable(tableName);
        filterList = getExtPropertyFilters(m, filterList);
        boolean isFindKeyOnly = HBaseUtils.isFindKeyOnly(m, columnsToOutput);
        filterList = onFindKeyOnly(filterList, isFindKeyOnly);
        List<HBaseDataWrapper> results = hbaseReader.loadData(hTable, rowKey, startRow, endRow, null, filterList,
                !isFindKeyOnly ? columnsToOutput : new ArrayList<Map<String, Object>>());
        return onRead(m, columnsToOutput, hTable, results);
    }

    /**
     * On find key only.
     * 
     * @param filterList
     *            the filter list
     * @param isFindKeyOnly
     *            the is find key only
     * @return the filter list
     */
    private FilterList onFindKeyOnly(FilterList filterList, boolean isFindKeyOnly)
    {
        if (isFindKeyOnly)
        {
            if (filterList == null)
            {
                filterList = new FilterList();
            }
            filterList.addFilter(new KeyOnlyFilter());
        }
        return filterList;
    }

    /**
     * Gets the ext property filters.
     * 
     * @param m
     *            the m
     * @param filterList
     *            the filter list
     * @return the ext property filters
     */
    private FilterList getExtPropertyFilters(EntityMetadata m, FilterList filterList)
    {
        Filter filter = getFilter(m.getTableName());
        if (filter != null)
        {
            if (filterList == null)
            {
                filterList = new FilterList();
            }
            filterList.addFilter(filter);
        }
        return filterList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#writeData(java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * java.lang.Object, java.util.List, boolean)
     */
    @Override
    public void writeData(String tableName, EntityMetadata m, Object entity, Object rowId,
            List<RelationHolder> relations, boolean showQuery) throws IOException
    {
        HBaseRow hbaseRow = createHbaseRow(m, entity, rowId, relations);
        writeHbaseRowInATable(tableName, hbaseRow);
    }

    /**
     * Creates the hbase row.
     * 
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param rowId
     *            the row id
     * @param relations
     *            the relations
     * @return the hBase row
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public HBaseRow createHbaseRow(EntityMetadata m, Object entity, Object rowId, List<RelationHolder> relations)
            throws IOException
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            rowId = KunderaCoreUtils.prepareCompositeKey(m, rowId);
        }
        HBaseRow hbaseRow = new HBaseRow(rowId, new ArrayList<HBaseCell>());
        // handle attributes and embeddables
        createCellsAndAddToRow(entity, metaModel, attributes, hbaseRow, m, -1, "");
        // handle relations
        if (relations != null && !relations.isEmpty())
        {
            hbaseRow.addCells(getRelationCell(m, rowId, relations));
        }
        // handle inheritence
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();
        if (discrColumn != null && discrValue != null)
        {
            hbaseRow.addCell(new HBaseCell(m.getTableName(), discrColumn, discrValue));
        }
        return hbaseRow;
    }

    /**
     * Gets the relation cell.
     * 
     * @param m
     *            the m
     * @param rowId
     *            the row id
     * @param relations
     *            the relations
     * @return the relation cell
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<HBaseCell> getRelationCell(EntityMetadata m, Object rowId, List<RelationHolder> relations)
            throws IOException
    {
        List<HBaseCell> relationCells = new ArrayList<HBaseCell>();
        for (RelationHolder relation : relations)
        {
            HBaseCell hBaseCell = new HBaseCell(m.getTableName(), relation.getRelationName(),
                    relation.getRelationValue());
            relationCells.add(hBaseCell);
        }
        return relationCells;
    }

    /**
     * Write hbase row in a table.
     * 
     * @param tableName
     *            the table name
     * @param hbaseRow
     *            the hbase row
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void writeHbaseRowInATable(String tableName, HBaseRow hbaseRow) throws IOException
    {
        Table hTable = gethTable(tableName);
        ((HBaseWriter) hbaseWriter).writeRow(hTable, hbaseRow);
        hTable.close();
    }

    /**
     * Creates the cells and add to row.
     * 
     * @param entity
     *            the entity
     * @param metaModel
     *            the meta model
     * @param attributes
     *            the attributes
     * @param hbaseRow
     *            the hbase row
     * @param m
     *            the m
     * @param count
     *            the count
     * @param prefix
     *            the prefix
     */
    private void createCellsAndAddToRow(Object entity, MetamodelImpl metaModel, Set<Attribute> attributes,
            HBaseRow hbaseRow, EntityMetadata m, int count, String prefix)
    {
        AbstractAttribute idCol = (AbstractAttribute) m.getIdAttribute();
        for (Attribute attribute : attributes)
        {
            AbstractAttribute absAttrib = (AbstractAttribute) attribute;
            Class clazz = absAttrib.getBindableJavaType();
            if (metaModel.isEmbeddable(clazz))
            {
                Set<Attribute> attribEmbeddables = metaModel.embeddable(absAttrib.getBindableJavaType())
                        .getAttributes();
                Object embeddedField = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
                if (attribute.isCollection() && embeddedField != null)
                {
                    int newCount = count + 1;
                    String newPrefix = prefix != "" ? prefix + absAttrib.getJPAColumnName() + HBaseUtils.DELIM
                            : absAttrib.getJPAColumnName() + HBaseUtils.DELIM;
                    List listOfEmbeddables = (List) embeddedField;
                    addColumnForCollectionSize(hbaseRow, listOfEmbeddables.size(), newPrefix, m.getTableName());
                    for (Object obj : listOfEmbeddables)
                    {
                        createCellsAndAddToRow(obj, metaModel, attribEmbeddables, hbaseRow, m, newCount++, newPrefix);
                    }
                }
                else if (embeddedField != null)
                {
                    String newPrefix = prefix != "" ? prefix + absAttrib.getJPAColumnName() + HBaseUtils.DOT
                            : absAttrib.getJPAColumnName() + HBaseUtils.DOT;
                    createCellsAndAddToRow(embeddedField, metaModel, attribEmbeddables, hbaseRow, m, count, newPrefix);
                }
            }
            else if (!attribute.isCollection() && !attribute.isAssociation())
            {
                String columnFamily = absAttrib.getTableName() != null ? absAttrib.getTableName() : m.getTableName();
                String columnName = absAttrib.getJPAColumnName();
                columnName = count != -1 ? prefix + columnName + HBaseUtils.DELIM + count : prefix + columnName;
                Object value = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
                HBaseCell hbaseCell = new HBaseCell(columnFamily, columnName, value);
                if (!idCol.getName().equals(attribute.getName()) && value != null)
                {
                    hbaseRow.addCell(hbaseCell);
                }
            }
        }
    }

    private void addColumnForCollectionSize(HBaseRow hbaseRow, int size, String prefix, String colFamily)
    {
        String columnName = prefix + HBaseUtils.SIZE;
        HBaseCell hbaseCell = new HBaseCell(colFamily, columnName, size);
        hbaseRow.addCell(hbaseCell);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#writeJoinTableData(java.lang
     * .String, java.lang.String, java.util.Map)
     */
    @Override
    public void writeJoinTableData(String tableName, Object rowId, Map<String, Object> columns, String columnFamilyName)
            throws IOException
    {
        Table hTable = gethTable(tableName);

        hbaseWriter.writeColumns(hTable, rowId, columns, columnFamilyName);

        closeHTable(hTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#getForeignKeysFromJoinTable
     * (java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String schemaName, String joinTableName, Object rowKey,
            String inverseJoinColumnName)
    {
        List<E> foreignKeys = new ArrayList<E>();
        Table hTable = null;
        String tableName = HBaseUtils.getHTableName(schemaName, joinTableName);
        try
        {
            hTable = gethTable(tableName);
            List<HBaseDataWrapper> results = hbaseReader.loadData(hTable, rowKey, null, null, joinTableName,
                    getFilter(joinTableName), null);
            if (results != null && !results.isEmpty())
            {
                HBaseDataWrapper data = results.get(0);
                Map<String, byte[]> hbaseValues = data.getColumns();
                Set<String> columnNames = hbaseValues.keySet();
                for (String columnName : columnNames)
                {
                    if (columnName.startsWith(HBaseUtils.getColumnDataKey(joinTableName, inverseJoinColumnName)))
                    {
                        byte[] columnValue = data.getColumnValue(columnName);
                        String hbaseColumnValue = Bytes.toString(columnValue);
                        foreignKeys.add((E) hbaseColumnValue);
                    }
                }
            }
        }
        catch (IOException e)
        {
            return foreignKeys;
        }
        finally
        {
            try
            {
                if (hTable != null)
                {
                    closeHTable(hTable);
                }
            }
            catch (IOException e)
            {
                logger.error("Error in closing hTable, caused by: ", e);
            }
        }
        return foreignKeys;
    }

    /**
     * Gets the h table.
     * 
     * @param tableName
     *            the table name
     * @return the h table
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public Table gethTable(final String tableName) throws IOException
    {
        return connection.getTable(TableName.valueOf(tableName));
    }

    /**
     * Puth table.
     * 
     * @param hTable
     *            the h table
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void closeHTable(Table hTable) throws IOException
    {
        hTable.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.hbase.admin.DataHandler#shutdown()
     */
    @Override
    public void shutdown()
    {
    }

    /**
     * Populate entity from hBase data.
     * 
     * @param entity
     *            the entity
     * @param hbaseData
     *            the hbase data
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @return the object
     */
    private Object populateEntityFromHBaseData(Object entity, HBaseDataWrapper hbaseData, EntityMetadata m,
            Object rowKey)
    {
        try
        {
            Map<String, Object> relations = new HashMap<String, Object>();
            if (entity.getClass().isAssignableFrom(EnhanceEntity.class))
            {
                relations = ((EnhanceEntity) entity).getRelations();
                entity = ((EnhanceEntity) entity).getEntity();
            }
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());
            Set<Attribute> attributes = ((AbstractManagedType) entityType).getAttributes();

            writeValuesToEntity(entity, hbaseData, m, metaModel, attributes, m.getRelationNames(), relations, -1, "");
            if (!relations.isEmpty())
            {
                return new EnhanceEntity(entity, rowKey, relations);
            }
            return entity;
        }
        catch (PropertyAccessException e1)
        {
            throw new RuntimeException(e1);
        }
    }

    /**
     * Write values to entity.
     * 
     * @param entity
     *            the entity
     * @param hbaseData
     *            the hbase data
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param attributes
     *            the attributes
     * @param relationNames
     *            the relation names
     * @param relations
     *            the relations
     * @param count
     *            the count
     * @param prefix
     *            the prefix
     * @return the int
     */
    private void writeValuesToEntity(Object entity, HBaseDataWrapper hbaseData, EntityMetadata m,
            MetamodelImpl metaModel, Set<Attribute> attributes, List<String> relationNames,
            Map<String, Object> relations, int count, String prefix)
    {
        for (Attribute attribute : attributes)
        {
            Class javaType = ((AbstractAttribute) attribute).getBindableJavaType();
            if (metaModel.isEmbeddable(javaType))
            {
                processEmbeddable(entity, hbaseData, m, metaModel, count, prefix, attribute, javaType);
            }
            else if (!attribute.isCollection())
            {
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                columnName = count != -1 ? prefix + columnName + HBaseUtils.DELIM + count : prefix + columnName;
                String idColName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
                String colFamily = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                        .getTableName() : m.getTableName();
                byte[] columnValue = hbaseData.getColumnValue(HBaseUtils.getColumnDataKey(colFamily, columnName));
                if (relationNames != null && relationNames.contains(columnName) && columnValue != null
                        && columnValue.length > 0)
                {
                    EntityType entityType = metaModel.entity(m.getEntityClazz());
                    relations.put(columnName, getObjectFromByteArray(entityType, columnValue, columnName, m));
                }
                else if (!idColName.equals(columnName) && columnValue != null)
                {
                    PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), columnValue);
                }
            }
        }
    }

    /**
     * Process embeddable.
     * 
     * @param entity
     *            the entity
     * @param hbaseData
     *            the hbase data
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param count
     *            the count
     * @param prefix
     *            the prefix
     * @param attribute
     *            the attribute
     * @param javaType
     *            the java type
     */
    private void processEmbeddable(Object entity, HBaseDataWrapper hbaseData, EntityMetadata m,
            MetamodelImpl metaModel, int count, String prefix, Attribute attribute, Class javaType)
    {
        Set<Attribute> attribEmbeddables = metaModel.embeddable(javaType).getAttributes();
        Object embeddedField = KunderaCoreUtils.createNewInstance(javaType);
        if (!attribute.isCollection())
        {
            String newPrefix = prefix != "" ? prefix + ((AbstractAttribute) attribute).getJPAColumnName()
                    + HBaseUtils.DOT : ((AbstractAttribute) attribute).getJPAColumnName() + HBaseUtils.DOT;
            writeValuesToEntity(embeddedField, hbaseData, m, metaModel, attribEmbeddables, null, null, count, newPrefix);
            PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), embeddedField);
        }
        else
        {
            int newCount = count + 1;
            String newPrefix = prefix != "" ? prefix + ((AbstractAttribute) attribute).getJPAColumnName()
                    + HBaseUtils.DELIM : ((AbstractAttribute) attribute).getJPAColumnName() + HBaseUtils.DELIM;
            List embeddedCollection = new ArrayList();
            byte[] columnValue = hbaseData.getColumnValue(HBaseUtils.getColumnDataKey(m.getTableName(), newPrefix
                    + HBaseUtils.SIZE));
            int size = 0;
            if (columnValue != null)
            {
                size = Bytes.toInt(columnValue);
            }
            while (size != newCount)
            {
                embeddedField = KunderaCoreUtils.createNewInstance(javaType);
                writeValuesToEntity(embeddedField, hbaseData, m, metaModel, attribEmbeddables, null, null, newCount++,
                        newPrefix);
                embeddedCollection.add(embeddedField);
            }
            PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), embeddedCollection);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#deleteRow(java.lang.String,
     * java.lang.String)
     */
    @Override
    public void deleteRow(Object rowKey, String colName, String colFamily, String tableName) throws IOException
    {
        Table hTable = gethTable(tableName);
        hbaseWriter.delete(hTable, rowKey, colFamily, colName);
        closeHTable(hTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#findParentEntityFromJoinTable
     * (com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public List<Object> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId)
    {
        throw new PersistenceException("Not applicable for HBase");
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public void setFilter(Filter filter)
    {
        if (this.filter == null)
        {
            this.filter = new FilterList();
        }
        if (filter != null)
        {
            this.filter.addFilter(filter);
        }
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
        FilterList filterList = this.filters.get(columnFamily);
        if (filterList == null)
        {
            filterList = new FilterList();
        }
        if (filter != null)
        {
            filterList.addFilter(filter);
        }
        this.filters.put(columnFamily, filterList);
    }

    /**
     * On read.
     * 
     * @param m
     *            the m
     * @param columnsToOutput
     *            the columns to output
     * @param hTable
     *            the h table
     * @param results
     *            the results
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List onRead(EntityMetadata m, List<Map<String, Object>> columnsToOutput, Table hTable,
            List<HBaseDataWrapper> results) throws IOException
    {
        Class clazz = m.getEntityClazz();
        List outputResults = new ArrayList();
        try
        {
            if (results != null)
            {
                return columnsToOutput != null && !columnsToOutput.isEmpty() ? returnSpecificFieldList(m,
                        columnsToOutput, results, outputResults) : returnEntityObjectList(m, results, outputResults);
            }
        }
        catch (Exception e)
        {
            logger.error("Error while creating an instance of {}, Caused by: .", clazz, e);
            throw new PersistenceException(e);
        }
        finally
        {
            if (hTable != null)
            {
                closeHTable(hTable);
            }
        }
        return outputResults;
    }

    /**
     * Return entity object list.
     * 
     * @param m
     *            the m
     * @param results
     *            the results
     * @param outputResults
     *            the output results
     * @return the list
     */
    private List returnEntityObjectList(EntityMetadata m, List<HBaseDataWrapper> results, List outputResults)
    {
        Map<Object, Object> entityListMap = new HashMap<Object, Object>();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        List<AbstractManagedType> subManagedTypes = ((AbstractManagedType) entityType).getSubManagedType();
        Map<String, Class> discrValueToEntityClazz = new HashMap<String, Class>();
        String discrColumn = null;
        if (subManagedTypes != null && !subManagedTypes.isEmpty())
        {
            for (AbstractManagedType subEntity : subManagedTypes)
            {
                discrColumn = ((AbstractManagedType) subEntity).getDiscriminatorColumn();
                discrValueToEntityClazz.put(subEntity.getDiscriminatorValue(), subEntity.getJavaType());
            }
        }
        for (HBaseDataWrapper data : results)
        {
            Class clazz = null;
            if (discrValueToEntityClazz != null && !discrValueToEntityClazz.isEmpty())
            {
                String discrColumnKey = HBaseUtils.getColumnDataKey(m.getTableName(), discrColumn);
                String discrValue = Bytes.toString(data.getColumnValue(discrColumnKey));
                clazz = discrValueToEntityClazz.get(discrValue);
                m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
            }
            else
            {
                clazz = m.getEntityClazz();
            }

            Object entity = KunderaCoreUtils.createNewInstance(clazz); // Entity
            Object rowKeyValue = HBaseUtils.fromBytes(m, metaModel, data.getRowKey());
            if (!metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
            {
                PropertyAccessorHelper.setId(entity, m, rowKeyValue);
            }
            if (entityListMap.get(rowKeyValue) != null)
            {
                entity = entityListMap.get(rowKeyValue);
            }
            entity = populateEntityFromHBaseData(entity, data, m, null);
            if (entity != null)
            {
                entityListMap.put(rowKeyValue, entity);
            }
        }
        for (Object obj : entityListMap.values())
        {
            outputResults.add(obj);
        }
        return outputResults;
    }

    /**
     * Return specific field list.
     * 
     * @param m
     *            the m
     * @param columnsToOutput
     *            the columns to output
     * @param results
     *            the results
     * @param outputResults
     *            the output results
     * @return the list
     */
    private List returnSpecificFieldList(EntityMetadata m, List<Map<String, Object>> columnsToOutput,
            List<HBaseDataWrapper> results, List outputResults)
    {
        for (HBaseDataWrapper data : results)
        {
            List result = new ArrayList();
            Map<String, byte[]> columns = data.getColumns();
            for (Map<String, Object> map : columnsToOutput)
            {
                Object obj;
                String colDataKey = HBaseUtils.getColumnDataKey((String) map.get(Constants.COL_FAMILY),
                        (String) map.get(Constants.DB_COL_NAME));
                if ((boolean) map.get(Constants.IS_EMBEDDABLE))
                {
                    Class embedClazz = (Class) map.get(Constants.FIELD_CLAZZ);
                    String prefix = (String) map.get(Constants.DB_COL_NAME) + HBaseUtils.DOT;
                    obj = populateEmbeddableObject(data, KunderaCoreUtils.createNewInstance(embedClazz), m, embedClazz,
                            prefix);
                }
                else if (isIdCol(m, (String) map.get(Constants.DB_COL_NAME)))
                {
                    obj = HBaseUtils.fromBytes(data.getRowKey(), (Class) map.get(Constants.FIELD_CLAZZ));
                }
                else
                {
                    obj = HBaseUtils.fromBytes(columns.get(colDataKey), (Class) map.get(Constants.FIELD_CLAZZ));
                }
                result.add(obj);
            }
            if (columnsToOutput.size() == 1)
                outputResults.addAll(result);
            else
                outputResults.add(result);
        }
        return outputResults;
    }

    /**
     * Checks if is id col.
     * 
     * @param m
     *            the m
     * @param colName
     *            the col name
     * @return true, if is id col
     */
    private boolean isIdCol(EntityMetadata m, String colName)
    {
        return ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equals(colName);
    }

    /**
     * Populate embeddable object.
     * 
     * @param data
     *            the data
     * @param obj
     *            the obj
     * @param m
     *            the m
     * @param clazz
     *            the clazz
     * @param prefix
     * @return the object
     */
    private Object populateEmbeddableObject(HBaseDataWrapper data, Object obj, EntityMetadata m, Class clazz,
            String prefix)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        Set<Attribute> attributes = metaModel.embeddable(clazz).getAttributes();
        writeValuesToEntity(obj, data, m, metaModel, attributes, null, null, -1, prefix);
        return obj;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#preparePut(com.impetus.client
     * .hbase.admin.HBaseRow)
     */
    @Override
    public Put preparePut(HBaseRow hbaseRow)
    {
        return ((HBaseWriter) hbaseWriter).preparePut(hbaseRow);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#scanRowyKeys(org.apache.hadoop
     * .hbase.filter.FilterList, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Class)
     */
    @Override
    public Object[] scanRowyKeys(FilterList filterList, String tableName, String columnFamilyName, String columnName,
            final Class rowKeyClazz) throws IOException
    {
        Table hTable = gethTable(tableName);
        return hbaseReader.scanRowKeys(hTable, filterList, columnFamilyName, columnName, rowKeyClazz);
    }

    /**
     * Gets the object from byte array.
     * 
     * @param entityType
     *            the entity type
     * @param value
     *            the value
     * @param jpaColumnName
     *            the jpa column name
     * @param m
     *            the m
     * @return the object from byte array
     */
    private Object getObjectFromByteArray(EntityType entityType, byte[] value, String jpaColumnName, EntityMetadata m)
    {
        if (jpaColumnName != null)
        {
            String fieldName = m.getFieldName(jpaColumnName);
            if (fieldName != null)
            {
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        attribute.getJavaType());
                Object colValue = PropertyAccessorHelper.getObject(relationMetadata.getIdAttribute().getJavaType(),
                        value);
                return colValue;
            }
        }
        logger.warn("No value found for column {}, returning null.", jpaColumnName);
        return null;
    }

    /**
     * Sets the fetch size.
     * 
     * @param fetchSize
     *            the new fetch size
     */
    public void setFetchSize(final int fetchSize)
    {
        ((HBaseReader) hbaseReader).setFetchSize(fetchSize);
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
        Object entity = null;
        HBaseDataWrapper result = ((HBaseReader) hbaseReader).next();
        List<HBaseDataWrapper> results = new ArrayList<HBaseDataWrapper>();
        results.add(result);
        try
        {
            List output = onRead(m, columnsToOutput,
                    gethTable(HBaseUtils.getHTableName(m.getSchema(), m.getTableName())), results);
            return output != null && !output.isEmpty() ? output.get(0) : output;
        }
        catch (IOException e)
        {
            logger.error("Error during finding next record, Caused by: .", e);
            throw new KunderaException(e);
        }
    }

    /**
     * Checks for next.
     * 
     * @return true, if successful
     */
    public boolean hasNext()
    {
        return ((HBaseReader) hbaseReader).hasNext();
    }

    /**
     * Reset.
     */
    public void reset()
    {
        resetFilter();
        ((HBaseReader) hbaseReader).reset();
    }

    /**
     * Reset filter.
     */
    public void resetFilter()
    {
        filter = null;
        filters = new ConcurrentHashMap<String, FilterList>();
    }

    /**
     * Gets the handle.
     * 
     * @return the handle
     */
    public HBaseDataHandler getHandle()
    {
        HBaseDataHandler handler = new HBaseDataHandler(this.kunderaMetadata, this.connection);
        handler.filter = this.filter;
        handler.filters = this.filters;
        return handler;
    }

    /**
     * Gets the filter.
     * 
     * @param columnFamily
     *            the column family
     * @return the filter
     */
    private Filter getFilter(final String columnFamily)
    {
        FilterList filter = filters.get(columnFamily);
        if (filter == null)
        {
            return this.filter;
        }
        if (this.filter != null)
        {
            filter.addFilter(this.filter);
        }
        return filter;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#prepareDelete(java.lang.Object
     * )
     */
    @Override
    public Row prepareDelete(Object rowKey)
    {
        byte[] rowBytes = HBaseUtils.getBytes(rowKey);
        return new Delete(rowBytes);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#batchProcess(java.util.Map)
     */
    @Override
    public void batchProcess(Map<String, List<Row>> batchData)
    {
        for (String tableName : batchData.keySet())
        {
            List<Row> actions = batchData.get(tableName);
            try
            {
                Table hTable = gethTable(tableName);
                hTable.batch(actions, new Object[actions.size()]);
            }
            catch (IOException | InterruptedException e)
            {
                logger.error("Error while batch processing on HTable: " + tableName);
                throw new PersistenceException(e);
            }

        }

    }
}