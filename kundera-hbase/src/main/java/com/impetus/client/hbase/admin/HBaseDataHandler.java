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
package com.impetus.client.hbase.admin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseData;
import com.impetus.client.hbase.Reader;
import com.impetus.client.hbase.Writer;
import com.impetus.client.hbase.service.HBaseReader;
import com.impetus.client.hbase.service.HBaseWriter;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class HBaseDataHandler.
 * 
 * @author vivek.mishra
 */
public class HBaseDataHandler implements DataHandler
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseDataHandler.class);

    /** The admin. */
    private HBaseAdmin admin;

    /** The conf. */
    private HBaseConfiguration conf;

    /** The h table pool. */
    private HTablePool hTablePool;

    /** The hbase reader. */
    private Reader hbaseReader = new HBaseReader();

    /** The hbase writer. */
    private Writer hbaseWriter = new HBaseWriter();

    private Filter filter;

    /**
     * Instantiates a new h base data handler.
     * 
     * @param conf
     *            the conf
     * @param hTablePool
     *            the h table pool
     */
    public HBaseDataHandler(HBaseConfiguration conf, HTablePool hTablePool)
    {
        try
        {
            this.conf = conf;
            this.hTablePool = hTablePool;
            this.admin = new HBaseAdmin(conf);
        }
        catch (Exception e)
        {
            // TODO We need a generic ExceptionTranslator
            throw new PersistenceException(e);
        }
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
            HTableDescriptor htDescriptor = new HTableDescriptor(tableName);
            for (String columnFamily : colFamily)
            {
                HColumnDescriptor familyMetadata = new HColumnDescriptor(columnFamily);
                htDescriptor.addFamily(familyMetadata);
            }

            admin.createTable(htDescriptor);
        }
    }

    /**
     * Adds the column family to table.
     * 
     * @param tableName
     *            the table name
     * @param columnFamilyName
     *            the column family name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void addColumnFamilyToTable(String tableName, String columnFamilyName) throws IOException
    {
        HColumnDescriptor cfDesciptor = new HColumnDescriptor(columnFamilyName);

        try
        {
            if (admin.tableExists(tableName))
            {

                // Before any modification to table schema, it's necessary to
                // disable it
                if (!admin.isTableEnabled(tableName))
                {
                    admin.enableTable(tableName);
                }
                HTableDescriptor descriptor = admin.getTableDescriptor(tableName.getBytes());
                boolean found = false;
                for (HColumnDescriptor hColumnDescriptor : descriptor.getColumnFamilies())
                {
                    if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(columnFamilyName))
                        found = true;
                }
                if (!found)
                {

                    if (admin.isTableEnabled(tableName))
                    {
                        admin.disableTable(tableName);
                    }

                    admin.addColumn(tableName, cfDesciptor);

                    // Enable table once done
                    admin.enableTable(tableName);
                }
            }
            else
            {
                log.warn("Table " + tableName + " doesn't exist, so no question of adding column family "
                        + columnFamilyName + " to it!");
            }
        }
        catch (IOException e)
        {
            log.error("Error while adding column family " + columnFamilyName + " to table " + tableName);
            throw e;
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
    public List readData(final String tableName, Class clazz, EntityMetadata m, final Object rowKey,
            List<String> relationNames) throws IOException
    {

        List output = null;

        Object entity = null;

        HTable hTable = null;

        hTable = gethTable(tableName);

        // Load raw data from HBase
        List<HBaseData> results = hbaseReader.LoadData(hTable, rowKey, this.filter);
        output = onRead(tableName, clazz, m, output, hTable, entity, relationNames, results);
        return output;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#readDataByRange(java.lang.
     * String, java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.util.List,
     * byte[], byte[])
     */
    @Override
    public List readDataByRange(String tableName, Class clazz, EntityMetadata m, byte[] startRow, byte[] endRow,
            String[] columns) throws IOException
    {
        List output = null;
        HTable hTable = null;
        Object entity = null;
        List<String> relationNames = m.getRelationNames();
        // Load raw data from HBase
        hTable = gethTable(tableName);
        List<HBaseData> results = hbaseReader.loadAll(hTable, this.filter, startRow, endRow, null, columns);
        output = onRead(tableName, clazz, m, output, hTable, entity, relationNames, results);

        return output;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#writeData(java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * java.lang.String, java.util.List)
     */
    @Override
    public void writeData(String tableName, EntityMetadata m, Object entity, Object rowId,
            List<RelationHolder> relations) throws IOException
    {

        HTable hTable = gethTable(tableName);

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        Map<String, EmbeddableType> columnFamilies = metaModel.getEmbeddables(m.getEntityClazz());
        Set<String> keys = metaModel.getEmbeddables(m.getEntityClazz()).keySet();

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();

        Map<String, Object> relationValue = null;

        HBaseDataWrapper columnWrapper = new HBaseDataWrapper(rowId, new java.util.HashSet<Attribute>(), entity, null);
        List<HBaseDataWrapper> persistentData = new ArrayList<HBaseDataHandler.HBaseDataWrapper>(attributes.size());

        preparePersistentData(tableName, entity, rowId, metaModel, attributes, columnWrapper, persistentData);

        hbaseWriter.writeColumns(hTable, columnWrapper.getRowKey(), columnWrapper.getColumns(), entity);

        for (HBaseDataWrapper wrapper : persistentData)
        {
            hbaseWriter.writeColumns(hTable, wrapper.getColumnFamily(), wrapper.getRowKey(), wrapper.getColumns(),
                    wrapper.getEntity());

        }

        // Persist relationships as a column in newly created Column family by
        // Kundera
        boolean containsEmbeddedObjectsOnly = columnWrapper.getColumns().isEmpty() && persistentData.isEmpty();

        if (relations != null && !relations.isEmpty())
        {
            hbaseWriter.writeRelations(hTable, rowId, containsEmbeddedObjectsOnly, relations);
        }

        puthTable(hTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#writeJoinTableData(java.lang
     * .String, java.lang.String, java.util.Map)
     */
    @Override
    public void writeJoinTableData(String tableName, Object rowId, Map<String, Object> columns) throws IOException
    {
        HTable hTable = gethTable(tableName);

        hbaseWriter.writeColumns(hTable, rowId, columns);

        puthTable(hTable);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#getForeignKeysFromJoinTable
     * (java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, Object rowKey, String inverseJoinColumnName)
    {
        List<E> foreignKeys = new ArrayList<E>();

        HTable hTable = null;

        // Load raw data from Join Table in HBase
        try
        {
            hTable = gethTable(joinTableName);

            List<HBaseData> results = hbaseReader.LoadData(hTable, Constants.JOIN_COLUMNS_FAMILY_NAME, rowKey, filter);

            // assuming rowKey is not null.
            if (results != null)
            {

                HBaseData data = results.get(0);

                List<KeyValue> hbaseValues = data.getColumns();
                if (hbaseValues != null)
                {
                    for (KeyValue colData : hbaseValues)
                    {
                        String hbaseColumn = Bytes.toString(colData.getQualifier());
                        String hbaseColumnFamily = Bytes.toString(colData.getFamily());

                        if (hbaseColumnFamily.equals(Constants.JOIN_COLUMNS_FAMILY_NAME)
                                && hbaseColumn.startsWith(inverseJoinColumnName))
                        {
                            byte[] val = colData.getValue();

                            // TODO : Because no attribute class is present, so
                            // cannot be done.
                            String hbaseColumnValue = Bytes.toString(val);

                            foreignKeys.add((E) hbaseColumnValue);
                        }
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
                    puthTable(hTable);
                }
            }
            catch (IOException e)
            {

                // Do nothing.
            }
        }
        return foreignKeys;
    }

    /**
     * Selects an HTable from the pool and returns.
     * 
     * @param tableName
     *            Name of HBase table
     * @return the h table
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public HTable gethTable(final String tableName) throws IOException
    {
        return (HTable) hTablePool.getTable(tableName);
    }

    /**
     * Puts HTable back into the HBase table pool.
     * 
     * @param hTable
     *            HBase Table instance
     */
    private void puthTable(HTable hTable) throws IOException
    {
        hTablePool.putTable(hTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.hbase.admin.DataHandler#shutdown()
     */
    @Override
    public void shutdown()
    {

        // TODO: Shutting down admin actually shuts down HMaster, something we
        // don't want.
        // Devise a better way to release resources.

        /*
         * try {
         * 
         * admin.shutdown();
         * 
         * } catch (IOException e) { throw new RuntimeException(e.getMessage());
         * }
         */
    }

    // TODO: Scope of performance improvement in this method
    /**
     * Populate entity from hbase data.
     * 
     * @param entity
     *            the entity
     * @param hbaseData
     *            the hbase data
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param relationNames
     *            the relation names
     * @return the object
     */
    private Object populateEntityFromHbaseData(Object entity, HBaseData hbaseData, EntityMetadata m, Object rowKey,
            List<String> relationNames)
    {
        try
        {
            /* Set Row Key */

            PropertyAccessorHelper.setId(entity, m, HBaseUtils.fromBytes(m, hbaseData.getRowKey()));

            // Raw data retrieved from HBase for a particular row key (contains
            // all column families)
            List<KeyValue> hbaseValues = hbaseData.getColumns();

            Map<String, Object> relations = new HashMap<String, Object>();
            /*
             * Populate columns data
             */
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());

            // List<Column> columns = m.getColumnsAsList();
            Set<Attribute> columns = entityType.getAttributes();
            // for (Column column : columns)
            for (Attribute column : columns)
            {
                Class javaType = ((AbstractAttribute) column).getBindableJavaType();
                String key = ((AbstractAttribute) column).getJPAColumnName();
                if (metaModel.isEmbeddable(javaType))
                {

                    EmbeddableType columnFamily = metaModel.embeddable(javaType);

                    Field columnFamilyFieldInEntity = (Field) column.getJavaMember();
                    Class<?> columnFamilyClass = columnFamilyFieldInEntity.getType();

                    // Get a name->field map for columns in this column family
                    Map<String, Field> columnNameToFieldMap = MetadataUtils.createColumnsFieldMap(m, columnFamily);

                    // Column family can be either @Embedded or
                    // @EmbeddedCollection
                    if (Collection.class.isAssignableFrom(columnFamilyClass))
                    {

                        Field embeddedCollectionField = (Field) column.getJavaMember();
                        Object[] embeddedObjectArr = new Object[hbaseValues.size()];
                        Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);
                        int prevCFNameCounter = 0; // Previous CF name counter
                        for (KeyValue colData : hbaseValues)
                        {
                            String cfInHbase = Bytes.toString(colData.getFamily());
                            // Only populate those data from Hbase into entity
                            // that
                            // matches with column family name
                            // in the format <Collection field name>#<sequence
                            // count>
                            if (!cfInHbase.startsWith(key))
                            {
                                if (relationNames != null && relationNames.contains(cfInHbase))
                                {
                                    relations.put(cfInHbase,
                                            getObjectFromByteArray(entityType, colData.getValue(), cfInHbase, m));
                                }
                                continue;

                            }

                            String cfNamePostfix = MetadataUtils.getEmbeddedCollectionPostfix(cfInHbase);
                            int cfNameCounter = Integer.parseInt(cfNamePostfix);
                            if (cfNameCounter != prevCFNameCounter)
                            {
                                prevCFNameCounter = cfNameCounter;

                                // Fresh embedded object for the next column
                                // family
                                // in collection
                                embeddedObject = MetadataUtils
                                        .getEmbeddedGenericObjectInstance(embeddedCollectionField);
                            }

                            // Set Hbase data into the embedded object
                            setHBaseDataIntoObject(colData, columnFamilyFieldInEntity, columnNameToFieldMap,
                                    embeddedObject);

                            embeddedObjectArr[cfNameCounter] = embeddedObject;

                            // Save embedded object into Cache, needed while
                            // updation and deletion
                            ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(rowKey,
                                    embeddedObject, cfInHbase);
                        }

                        // Collection to hold column family objects
                        Collection embeddedCollection = MetadataUtils
                                .getEmbeddedCollectionInstance(embeddedCollectionField);
                        embeddedCollection.addAll(Arrays.asList(embeddedObjectArr));
                        embeddedCollection.removeAll(Collections.singletonList(null));
                        embeddedObjectArr = null; // Eligible for GC

                        // Now, set the embedded collection into entity
                        if (embeddedCollection != null && !embeddedCollection.isEmpty())
                        {
                            PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
                        }
                    }
                    else
                    {
                        Object columnFamilyObj = columnFamilyClass.newInstance();

                        for (KeyValue colData : hbaseValues)
                        {
                            String cfInHbase = Bytes.toString(colData.getFamily());

                            if (!cfInHbase.equals(key))
                            {
                                if (relationNames != null && relationNames.contains(cfInHbase))
                                {
                                    relations.put(cfInHbase,
                                            getObjectFromByteArray(entityType, colData.getValue(), cfInHbase, m));
                                }
                                continue;
                            }
                            // Set Hbase data into the column family object

                            String colName = Bytes.toString(colData.getQualifier());
                            byte[] columnValue = colData.getValue();

                            // Get Column from metadata
                            Field columnField = columnNameToFieldMap.get(colName);
                            if (columnField != null)
                            {
                                if (columnFamilyFieldInEntity.isAnnotationPresent(Embedded.class)
                                        || columnFamilyFieldInEntity.isAnnotationPresent(ElementCollection.class))
                                {
                                    PropertyAccessorHelper.set(columnFamilyObj, columnField,
                                            HBaseUtils.fromBytes(columnValue, columnField.getType()));
                                }
                                else
                                {
                                    columnFamilyObj = getObjectFromByteArray(entityType, columnValue, cfInHbase, m);
                                }
                            }
                        }
                        PropertyAccessorHelper.set(entity, columnFamilyFieldInEntity, columnFamilyObj);
                    }

                }
                else if (!column.getName().equals(m.getIdAttribute().getName()))
                {
                    Field columnField = (Field) column.getJavaMember();
                    String columnName = ((AbstractAttribute) column).getJPAColumnName();

                    for (KeyValue colData : hbaseValues)
                    {
                        String hbaseColumn = Bytes.toString(colData.getFamily());
                        // String colName = getColumnName(hbaseColumn);
                        String colName = hbaseColumn;
                        if (relationNames != null && relationNames.contains(colName))
                        {
                            relations.put(colName, getObjectFromByteArray(entityType, colData.getValue(), colName, m));
                        }
                        else if (colName != null && colName.equalsIgnoreCase(columnName.toLowerCase()))
                        {
                            byte[] hbaseColumnValue = colData.getValue();
                            PropertyAccessorHelper.set(entity, columnField,
                                    HBaseUtils.fromBytes(hbaseColumnValue, columnField.getType()));
                        }
                    }
                }
            }

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
        catch (InstantiationException e1)
        {
            throw new RuntimeException(e1);
        }
        catch (IllegalAccessException e1)
        {
            throw new RuntimeException(e1);
        }

    }

    /**
     * Sets the h base data into object.
     * 
     * @param colData
     *            the col data
     * @param columnFamilyField
     *            the column family field
     * @param columnNameToFieldMap
     *            the column name to field map
     * @param columnFamilyObj
     *            the column family obj
     * @throws PropertyAccessException
     *             the property access exception
     */
    private void setHBaseDataIntoObject(KeyValue colData, Field columnFamilyField,
            Map<String, Field> columnNameToFieldMap, Object columnFamilyObj) throws PropertyAccessException
    {

        String colName = Bytes.toString(colData.getQualifier());
        byte[] columnValue = colData.getValue();

        // Get Column from metadata
        Field columnField = columnNameToFieldMap.get(colName);
        if (columnField != null)
        {
            if (columnFamilyField.isAnnotationPresent(Embedded.class)
                    || columnFamilyField.isAnnotationPresent(ElementCollection.class))
            {
                PropertyAccessorHelper.set(columnFamilyObj, columnField, columnValue);
            }
            else
            {
                columnFamilyObj = HBaseUtils.fromBytes(columnValue, columnFamilyObj.getClass());
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.admin.DataHandler#deleteRow(java.lang.String,
     * java.lang.String)
     */
    public void deleteRow(Object rowKey, String tableName) throws IOException
    {
        hbaseWriter.delete(gethTable(tableName), rowKey, tableName);

    }

    @Override
    public List<Object> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId)
    {

        throw new PersistenceException("Not applicable for HBase");
    }

    /**
     * Set filter to data handler.
     * 
     * @param filter
     *            hbase filter.
     */
    public void setFilter(Filter filter)
    {
        this.filter = filter;
    }

    /**
     * 
     * @param tableName
     * @param clazz
     * @param m
     * @param startRow
     * @param endRow
     * @param output
     * @param hTable
     * @param entity
     * @param relationNames
     * @param results
     * @return
     * @throws IOException
     */
    private List onRead(String tableName, Class clazz, EntityMetadata m, List output, HTable hTable, Object entity,
            List<String> relationNames, List<HBaseData> results) throws IOException
    {
        try
        {
            // Populate raw data from HBase into entity

            if (results != null)
            {
                for (HBaseData data : results)
                {

                    if (data.getColumns() != null)
                    {
                        entity = clazz.newInstance(); // Entity Object
                        entity = populateEntityFromHbaseData(entity, data, m, null, relationNames);
                        if (output == null)
                        {
                            output = new ArrayList();
                        }
                        output.add(entity);
                    }
                }
            }
        }
        catch (InstantiationException iex)
        {
            log.error("Error while creating an instance of " + clazz);
            throw new PersistenceException(iex);
            // return enhancedEntity;
        }
        catch (IllegalAccessException iaex)
        {
            log.error("Illegal Access while reading data from " + tableName + ", Caused by:" + iaex.getMessage());
            throw new PersistenceException(iaex);
            // return enhancedEntity;
        }
        catch (Exception e)
        {
            log.error("Error while creating an instance of " + clazz);
            throw new PersistenceException(e);
        }
        finally
        {
            if (hTable != null)
            {
                puthTable(hTable);
            }
        }
        return output;
    }

    /**
     * @author vivek.mishra
     * 
     */
    public static class HBaseDataWrapper
    {
        Object rowKey;

        private Set<Attribute> columns;

        private Object entity;

        private String columnFamily;

        /**
         * @param rowKey
         * @param columns
         * @param entity
         * @param columnFamily
         */
        public HBaseDataWrapper(Object rowKey, Set<Attribute> columns, Object entity, String columnFamily)
        {
            super();
            this.rowKey = rowKey;
            this.columns = columns;
            this.entity = entity;
            this.columnFamily = columnFamily;
        }

        /**
         * @return the rowKey
         */
        public Object getRowKey()
        {
            return rowKey;
        }

        /**
         * @return the columns
         */
        public Set<Attribute> getColumns()
        {
            return columns;
        }

        /**
         * @return the entity
         */
        public Object getEntity()
        {
            return entity;
        }

        /**
         * @return the columnFamily
         */
        public String getColumnFamily()
        {
            return columnFamily;
        }

        public void addColumn(Attribute column)
        {
            columns.add(column);
        }
    }

    public List scanData(Filter f, final String tableName, Class clazz, EntityMetadata m, String qualifier)
            throws IOException, InstantiationException, IllegalAccessException
    {
        List returnedResults = new ArrayList();
        List<HBaseData> results = hbaseReader.loadAll(gethTable(tableName), f, null, null, null, null);
        if (results != null)
        {
            for (HBaseData row : results)
            {
                Object entity = clazz.newInstance();
                returnedResults.add(populateEntityFromHbaseData(entity, row, m, row.getRowKey(), m.getRelationNames()));
            }
        }

        return returnedResults;
    }

    @Override
    public Object[] scanRowyKeys(FilterList filterList, String tableName, String columnFamilyName, String columnName)
            throws IOException
    {
        HTable hTable = null;
        hTable = gethTable(tableName);
        return hbaseReader.scanRowKeys(hTable, filterList, columnFamilyName, columnName);
    }

    private Object getObjectFromByteArray(EntityType entityType, byte[] value, String jpaColumnName, EntityMetadata m)
    {
        if (jpaColumnName != null)
        {
            String fieldName = m.getFieldName(jpaColumnName);
            if (fieldName != null)
            {
                Attribute attribute = fieldName != null ? entityType.getAttribute(fieldName) : null;

                EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(attribute.getJavaType());
                Object colValue = PropertyAccessorHelper.getObject(relationMetadata.getIdAttribute().getJavaType(),
                        (byte[]) value);
                return colValue;
            }
        }

        log.warn("No value found for : " + jpaColumnName + " returning null");
        return null;
    }

    /**
     * 
     * @param tableName
     * @param entity
     * @param rowId
     * @param metaModel
     * @param attributes
     * @param columnWrapper
     * @param persistentData
     * @return
     * @throws IOException
     */
    public void preparePersistentData(String tableName, Object entity, Object rowId, MetamodelImpl metaModel,
            Set<Attribute> attributes, HBaseDataWrapper columnWrapper, List<HBaseDataWrapper> persistentData)
            throws IOException
    {
        for (Attribute column : attributes)
        {
            String fieldName = ((AbstractAttribute) column).getJPAColumnName();

            Class javaType = ((AbstractAttribute) column).getBindableJavaType();
            if (metaModel.isEmbeddable(javaType))
            {
                String columnFamilyName = ((AbstractAttribute) column).getJPAColumnName();
                Field columnFamilyField = (Field) column.getJavaMember();
                Object columnFamilyObject = null;
                try
                {
                    columnFamilyObject = PropertyAccessorHelper.getObject(entity, columnFamilyField);
                }
                catch (PropertyAccessException e1)
                {
                    log.error("Error while getting " + columnFamilyName + " field from entity " + entity);
                    throw new KunderaException(e1);
                }

                if (columnFamilyObject != null)
                {
                    // continue;
                    // }

                    Set<Attribute> columns = metaModel.embeddable(javaType).getAttributes();
                    if (column.isCollection())
                    {
                        String dynamicCFName = null;

                        ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();
                        // Check whether it's first time insert or updation
                        if (ecCacheHandler.isCacheEmpty())
                        { // First time insert
                            int count = 0;
                            for (Object obj : (Collection) columnFamilyObject)
                            {
                                dynamicCFName = columnFamilyName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;
                                addColumnFamilyToTable(tableName, dynamicCFName);

                                persistentData.add(new HBaseDataWrapper(rowId, columns, obj, dynamicCFName));
                                count++;
                            }

                        }
                        else
                        {
                            // Updation
                            // Check whether this object is already in cache,
                            // which
                            // means we already have a column family with that
                            // name
                            // Otherwise we need to generate a fresh column
                            // family
                            // name
                            int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(rowId);
                            for (Object obj : (Collection) columnFamilyObject)
                            {
                                dynamicCFName = ecCacheHandler.getElementCollectionObjectName(rowId, obj);
                                if (dynamicCFName == null)
                                { // Fresh row
                                    dynamicCFName = columnFamilyName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                            + (++lastEmbeddedObjectCount);

                                }
                                addColumnFamilyToTable(tableName, dynamicCFName);
                                persistentData.add(new HBaseDataWrapper(rowId, columns, obj, dynamicCFName));
                            }

                            // Clear embedded collection cache for GC
                            ecCacheHandler.clearCache();
                        }

                    }
                    else
                    {
                        // Write Column family which was Embedded object in
                        // entity
                        if (columnFamilyField.isAnnotationPresent(Embedded.class))
                        {
                            persistentData.add(new HBaseDataWrapper(rowId, columns, columnFamilyObject,
                                    columnFamilyName));
                        }
                        else
                        {
                            persistentData.add(new HBaseDataWrapper(rowId, columns, columnFamilyObject,
                                    columnFamilyName));
                        }

                    }
                }
            }
            else if (!column.isAssociation())
            {
                columnWrapper.addColumn(column);

            }
        }

    }

    /**
     * @param data
     * @throws IOException
     */
    public void batch_insert(Map<HTable, List<HBaseDataWrapper>> data) throws IOException
    {
        hbaseWriter.persistRows(data);

    }

}
