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

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.PersistenceException;

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
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseData;
import com.impetus.client.hbase.Reader;
import com.impetus.client.hbase.Writer;
import com.impetus.client.hbase.service.HBaseReader;
import com.impetus.client.hbase.service.HBaseWriter;
import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author impetus
 */
public class HBaseDataHandler implements DataHandler
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseDataHandler.class);

    private HBaseAdmin admin;

    private HBaseConfiguration conf;

    private HTablePool hTablePool;

    private Reader hbaseReader = new HBaseReader();

    private Writer hbaseWriter = new HBaseWriter();

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

    private void addColumnFamilyToTable(String tableName, String columnFamilyName) throws IOException
    {
        HColumnDescriptor cfDesciptor = new HColumnDescriptor(columnFamilyName);

        try
        {
            if (admin.tableExists(tableName))
            {

                // Before any modification to table schema, it's necessary to
                // disable it
                if (admin.isTableEnabled(tableName))
                {
                    admin.disableTable(tableName);
                }
                admin.addColumn(tableName, cfDesciptor);

                // Enable table once done
                admin.enableTable(tableName);
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

    @Override
    public Object readData(final String tableName, Class clazz, EntityMetadata m, final String rowKey,
            List<String> relationNames) throws IOException
    {

        Object entity = null;

        HTable hTable = null;

        try
        {
            entity = clazz.newInstance(); // Entity Object

            hTable = gethTable(tableName);

            // Load raw data from HBase
            HBaseData data = hbaseReader.LoadData(hTable, rowKey);

            // Populate raw data from HBase into entity
            entity = populateEntityFromHbaseData(entity, data, m, rowKey, relationNames);

            // Map to hold property-name=>foreign-entity relations
            // Map<String, Set<String>> foreignKeysMap = new HashMap<String,
            // Set<String>>();

            // Set entity object and foreign key map into enhanced entity and
            // return
            // enhancedEntity = (E) EntityResolver.getEnhancedEntity(entity,
            // rowKey, foreignKeysMap);
        }
        catch (InstantiationException e1)
        {
            log.error("Error while creating an instance of " + clazz);
            // return enhancedEntity;
        }
        catch (IllegalAccessException e1)
        {
            log.error("Illegal Access while reading data from " + tableName + ";Details: " + e1.getMessage());
            // return enhancedEntity;
        }
        finally
        {
            if (hTable != null)
            {
                puthTable(hTable);
            }
        }

        return entity;
    }

    @Override
    public void writeData(String tableName, EntityMetadata m, Object entity, String rowId,
            List<RelationHolder> relations) throws IOException
    {

        HTable hTable = gethTable(tableName);

        // Now persist column families in the table. For HBase, embedded columns
        // are called column families
        List<EmbeddedColumn> columnFamilies = m.getEmbeddedColumnsAsList();

        for (EmbeddedColumn columnFamily : columnFamilies)
        {
            String columnFamilyName = columnFamily.getName();
            Field columnFamilyField = columnFamily.getField();
            Object columnFamilyObject = null;
            try
            {
                columnFamilyObject = PropertyAccessorHelper.getObject(entity/*
                                                                             * .
                                                                             * getEntity
                                                                             * (
                                                                             * )
                                                                             */, columnFamilyField);
            }
            catch (PropertyAccessException e1)
            {
                log.error("Error while getting " + columnFamilyName + " field from entity " + entity);
                return;
            }

            if (columnFamilyObject == null)
            {
                continue;
            }

            List<Column> columns = columnFamily.getColumns();

            // TODO: Handle Embedded collections differently
            // Write Column family which was Embedded collection in entity

            if (columnFamilyObject instanceof Collection)
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

                        hbaseWriter.writeColumns(hTable, dynamicCFName, rowId, columns, obj);
                        count++;
                    }

                }
                else
                {
                    // Updation
                    // Check whether this object is already in cache, which
                    // means we already have a column family with that name
                    // Otherwise we need to generate a fresh column family name
                    int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(rowId);
                    for (Object obj : (Collection) columnFamilyObject)
                    {
                        dynamicCFName = ecCacheHandler.getElementCollectionObjectName(rowId, obj);
                        if (dynamicCFName == null)
                        { // Fresh row
                            dynamicCFName = columnFamilyName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                    + (++lastEmbeddedObjectCount);
                        }
                    }

                    // Clear embedded collection cache for GC
                    ecCacheHandler.clearCache();
                }

            }
            else
            {
                // Write Column family which was Embedded object in entity
                if (columnFamilyField.isAnnotationPresent(Embedded.class))
                {
                    hbaseWriter.writeColumns(hTable, columnFamilyName, rowId, columns, columnFamilyObject);
                }
                else
                {
                    hbaseWriter.writeColumn(hTable, columnFamilyName, rowId, columns.get(0), columnFamilyObject);
                }

            }

        }

        // HBase tables may have columns alongwith column families
        List<Column> columns = m.getColumnsAsList();
        if (columns != null && !columns.isEmpty())
        {

            hbaseWriter.writeColumns(hTable, rowId, columns, entity);
        }

        // Persist relationships as a column in newly created Column family by
        // Kundera
        boolean containsEmbeddedObjectsOnly = (columns == null || columns.isEmpty());
        if (relations != null && !relations.isEmpty())
        {
            hbaseWriter.writeRelations(hTable, rowId, containsEmbeddedObjectsOnly, relations);
        }

        puthTable(hTable);
    }

    @Override
    public void writeJoinTableData(String tableName, String rowId, Map<String, String> columns) throws IOException
    {
        HTable hTable = gethTable(tableName);

        hbaseWriter.writeColumns(hTable, rowId, columns);

        puthTable(hTable);

    }

    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String rowKey, String inverseJoinColumnName)
    {
        List<E> foreignKeys = new ArrayList<E>();

        HTable hTable = null;

        // Load raw data from Join Table in HBase
        try
        {
            hTable = gethTable(joinTableName);

            HBaseData data = hbaseReader.LoadData(hTable, Constants.JOIN_COLUMNS_FAMILY_NAME, rowKey);
            List<KeyValue> hbaseValues = data.getColumns();

            for (KeyValue colData : hbaseValues)
            {
                String hbaseColumn = Bytes.toString(colData.getColumn());

                if (hbaseColumn.startsWith(Constants.JOIN_COLUMNS_FAMILY_NAME + ":" + inverseJoinColumnName))
                {
                    byte[] val = colData.getValue();
                    String hbaseColumnValue = Bytes.toString(val);

                    foreignKeys.add((E) hbaseColumnValue);
                }
            }
        }
        catch (IOException e)
        {
            return foreignKeys;
        }
        finally
        {
            if (hTable != null)
            {
                puthTable(hTable);
            }
        }
        return foreignKeys;
    }

    /**
     * Selects an HTable from the pool and returns
     * 
     * @param tableName
     *            Name of HBase table
     */
    private HTable gethTable(final String tableName) throws IOException
    {
        // return new HTable(conf, tableName);
        return hTablePool.getTable(tableName);
    }

    /**
     * Puts HTable back into the HBase table pool
     * 
     * @param hTable
     *            HBase Table instance
     */
    private void puthTable(HTable hTable)
    {
        hTablePool.putTable(hTable);
    }

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
    private Object populateEntityFromHbaseData(Object entity, HBaseData hbaseData, EntityMetadata m, String rowKey,
            List<String> relationNames)
    {
        try
        {
            /* Set Row Key */
            PropertyAccessorHelper.setId(entity, m, rowKey);

            // Raw data retrieved from HBase for a particular row key (contains
            // all column families)
            List<KeyValue> hbaseValues = hbaseData.getColumns();

            Map<String, Object> relations = new HashMap<String, Object>();
            /*
             * Populate columns data
             */
            List<Column> columns = m.getColumnsAsList();
            for (Column column : columns)
            {
                Field columnField = column.getField();
                String columnName = column.getName();

                for (KeyValue colData : hbaseValues)
                {
                    String hbaseColumn = Bytes.toString(colData.getColumn());
                    String colName = getColumnName(hbaseColumn);
                    if (hbaseColumn != null && hbaseColumn.startsWith(columnName))
                    {
                        byte[] hbaseColumnValue = colData.getValue();
                        PropertyAccessorHelper.set(entity, columnField, hbaseColumnValue);

                        break;
                    }
                    else if (relationNames != null && relationNames.contains(getColumnName(colName)))
                    {
                        relations.put(colName, Bytes.toString(colData.getValue()));
                    }
                }

            }

            /*
             * Set each column families, for HBase embedded columns are called
             * columns families
             */
            List<EmbeddedColumn> columnFamilies = m.getEmbeddedColumnsAsList();
            for (EmbeddedColumn columnFamily : columnFamilies)
            {
                Field columnFamilyFieldInEntity = columnFamily.getField();
                Class<?> columnFamilyClass = columnFamilyFieldInEntity.getType();

                // Get a name->field map for columns in this column family
                Map<String, Field> columnNameToFieldMap = MetadataUtils.createColumnsFieldMap(m, columnFamily);

                // Column family can be either @Embedded or @EmbeddedCollection
                if (Collection.class.isAssignableFrom(columnFamilyClass))
                {

                    Field embeddedCollectionField = columnFamily.getField();
                    Object[] embeddedObjectArr = new Object[hbaseValues.size()]; // Array
                                                                                 // to
                                                                                 // hold
                                                                                 // column
                                                                                 // family
                                                                                 // objects

                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);
                    int prevCFNameCounter = 0; // Previous CF name counter
                    for (KeyValue colData : hbaseValues)
                    {
                        String cfInHbase = Bytes.toString(colData.getFamily());
                        // Only populate those data from Hbase into entity that
                        // matches with column family name
                        // in the format <Collection field name>#<sequence
                        // count>
                        if (!cfInHbase.startsWith(columnFamily.getName()))
                        {
                            if (relationNames != null && relationNames.contains(cfInHbase))
                            {
                                relations.put(cfInHbase, Bytes.toString(colData.getValue()));
                            }
                            continue;

                        }

                        String cfNamePostfix = MetadataUtils.getEmbeddedCollectionPostfix(cfInHbase);
                        int cfNameCounter = Integer.parseInt(cfNamePostfix);
                        if (cfNameCounter != prevCFNameCounter)
                        {
                            prevCFNameCounter = cfNameCounter;

                            // Fresh embedded object for the next column family
                            // in collection
                            embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);
                        }

                        // Set Hbase data into the embedded object
                        setHBaseDataIntoObject(colData, columnFamilyFieldInEntity, columnNameToFieldMap, embeddedObject);

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

                        if (!cfInHbase.equals(columnFamily.getName()))
                        {
                            if (relationNames != null && relationNames.contains(cfInHbase))
                            {
                                relations.put(cfInHbase, Bytes.toString(colData.getValue()));
                            }
                            continue;

                        }
                        // Set Hbase data into the column family object
                        // setHBaseDataIntoObject(colData,
                        // columnFamilyFieldInEntity, columnNameToFieldMap,
                        // columnFamilyObj);

                        String colName = Bytes.toString(colData.getQualifier());
                        byte[] columnValue = colData.getValue();

                        // Get Column from metadata
                        Field columnField = columnNameToFieldMap.get(colName);
                        if (columnField != null)
                        {
                            if (columnFamilyFieldInEntity.isAnnotationPresent(Embedded.class)
                                    || columnFamilyFieldInEntity.isAnnotationPresent(ElementCollection.class))
                            {
                                PropertyAccessorHelper.set(columnFamilyObj, columnField, columnValue);
                            }
                            else
                            {
                                columnFamilyObj = Bytes.toString(columnValue);
                            }
                        }

                    }
                    PropertyAccessorHelper.set(entity, columnFamilyFieldInEntity, columnFamilyObj);

                }

            }
            if (!relations.isEmpty())
            {
                return new EnhanceEntity(entity, rowKey, relations);
            } else {
            return new EnhanceEntity(entity, rowKey, null);
            }

            
        }
        catch (PropertyAccessException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }
        catch (InstantiationException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }
        catch (IllegalAccessException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }

    }

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
                columnFamilyObj = Bytes.toString(columnValue);
            }
        }

    }

    private String getColumnName(String hbaseColumn)
    {
        return hbaseColumn != null ? hbaseColumn.substring(hbaseColumn.indexOf(0) + 1) : null;
    }

    public void deleteRow(String rowKey, String tableName) throws IOException
    {
        hbaseWriter.delete(gethTable(tableName), rowKey, tableName);

    }
}