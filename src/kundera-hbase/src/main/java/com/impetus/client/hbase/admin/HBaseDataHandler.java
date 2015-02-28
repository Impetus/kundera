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
import javax.persistence.EmbeddedId;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class HBaseDataHandler.
 * 
 * @author vivek.mishra
 */
public class HBaseDataHandler implements DataHandler
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseDataHandler.class);

    /** The admin. */
    private HBaseAdmin admin;

    /** The conf. */
    private Configuration conf;

    /** The h table pool. */
    private HTablePool hTablePool;

    /** The hbase reader. */
    private Reader hbaseReader = new HBaseReader();

    /** The hbase writer. */
    private Writer hbaseWriter = new HBaseWriter();

    private FilterList filter = null;

    private Map<String, FilterList> filters = new ConcurrentHashMap<String, FilterList>();

    private KunderaMetadata kunderaMetadata;

    /**
     * Instantiates a new h base data handler.
     * 
     * @param conf
     *            the conf
     * @param hTablePool
     *            the h table pool
     */
    public HBaseDataHandler(final KunderaMetadata kunderaMetadata, Configuration conf, HTablePool hTablePool)
    {
        try
        {
            this.kunderaMetadata = kunderaMetadata;
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
                log.warn("Table {} doesn't exist, so no question of adding column family {} to it!", tableName,
                        columnFamilyName);
            }
        }
        catch (IOException e)
        {
            log.error("Error while adding column family {}, to table{} . ", columnFamilyName, tableName);
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
            List<String> relationNames, FilterList f, String... columns) throws IOException
    {

        List output = null;

        Object entity = null;

        HTableInterface hTable = null;

        hTable = gethTable(tableName);

        if (getFilter(m.getTableName()) != null)
        {
            if (f == null)
            {
                f = new FilterList();
            }
            f.addFilter(getFilter(m.getTableName()));
        }

        // Load raw data from HBase
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());
        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(m.getTableName());
        Collections.shuffle(secondaryTables);
        List<HBaseData> results = new ArrayList<HBaseData>();
        for (String colTableName : secondaryTables)
        {
            results.addAll(hbaseReader.LoadData(hTable, colTableName, rowKey, f, columns));
        }

        output = onRead(tableName, clazz, m, output, hTable, entity, relationNames, results);
        return output;
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

        List output = null;

        Object entity = null;

        HTableInterface hTable = null;

        hTable = gethTable(tableName);

        // Load raw data from HBase
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());
        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(m.getTableName());
        List<HBaseData> results = new ArrayList<HBaseData>();
        for (String colTableName : secondaryTables)
        {
            List table = ((HBaseReader) hbaseReader).loadAll(hTable, rowKey, colTableName, columns);
            // null check for 'table'. addAll method throws exception if table
            // is null
            if (table != null)
            {
                results.addAll(table);
            }
        }

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
            String[] columns, FilterList f) throws IOException
    {
        List output = new ArrayList();
        HTableInterface hTable = null;
        Object entity = null;
        List<String> relationNames = m.getRelationNames();
        Filter filter = getFilter(m.getTableName());
        if (filter != null)
        {
            if (f == null)
            {
                f = new FilterList();
            }
            f.addFilter(filter);
        }
        // Load raw data from HBase
        hTable = gethTable(tableName);
        List<HBaseData> results = hbaseReader.loadAll(hTable, f, startRow, endRow, m.getTableName(), null, columns);
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
            List<RelationHolder> relations, boolean showQuery) throws IOException
    {
        HTableInterface hTable = gethTable(tableName);

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();

        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            rowId = KunderaCoreUtils.prepareCompositeKey(m, rowId);
        }

        HBaseDataWrapper columnWrapper = new HBaseDataWrapper(rowId, new java.util.HashMap<String, Attribute>(),
                entity, null);
        List<HBaseDataWrapper> persistentData = new ArrayList<HBaseDataHandler.HBaseDataWrapper>(attributes.size());

        Map<String, HBaseDataWrapper> columnWrappers = preparePersistentData(tableName, m.getTableName(), entity,
                rowId, metaModel, attributes, columnWrapper, persistentData, showQuery);

        writeColumnData(hTable, entity, columnWrappers);

        for (HBaseDataWrapper wrapper : persistentData)
        {
            hbaseWriter.writeColumns(hTable, wrapper.getColumnFamily(), wrapper.getRowKey(), wrapper.getColumns(),
                    wrapper.getValues(), wrapper.getEntity());
        }

        // Persist relationships as a column in newly created Column family by
        // Kundera
        boolean containsEmbeddedObjectsOnly = columnWrapper.getColumns().isEmpty() && persistentData.isEmpty();

        if (relations != null && !relations.isEmpty())
        {
            hbaseWriter.writeRelations(hTable, rowId, containsEmbeddedObjectsOnly, relations, m.getTableName());
        }

        // add discriminator column
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            List<RelationHolder> discriminator = new ArrayList<RelationHolder>(1);
            discriminator.add(new RelationHolder(discrColumn, discrValue));
            hbaseWriter.writeRelations(hTable, rowId, containsEmbeddedObjectsOnly, discriminator, m.getTableName());
        }

        puthTable(hTable);
    }

    private void writeColumnData(HTableInterface hTable, Object entity, Map<String, HBaseDataWrapper> columnWrappers)
            throws IOException
    {

        for (HBaseDataWrapper wrapper : columnWrappers.values())
        {
            hbaseWriter.writeColumns(hTable, wrapper.getRowKey(), wrapper.getColumns(), entity,
                    wrapper.getColumnFamily());
        }

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
        HTableInterface hTable = gethTable(tableName);

        hbaseWriter.writeColumns(hTable, rowId, columns, columnFamilyName);

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
    public <E> List<E> getForeignKeysFromJoinTable(String schemaName, String joinTableName, Object rowKey,
            String inverseJoinColumnName)
    {
        List<E> foreignKeys = new ArrayList<E>();

        HTableInterface hTable = null;

        // Load raw data from Join Table in HBase
        try
        {
            hTable = gethTable(schemaName);

            List<HBaseData> results = hbaseReader.LoadData(hTable, joinTableName, rowKey, getFilter(joinTableName));

            // assuming rowKey is not null.
            if (results != null)
            {

                HBaseData data = results.get(0);

                Map<String, byte[]> hbaseValues = data.getColumns();
                Set<String> columnNames = hbaseValues.keySet();

                for (String columnName : columnNames)
                {
                    if (columnName.startsWith(inverseJoinColumnName) && data.getColumnFamily().equals(joinTableName))
                    {
                        byte[] columnValue = data.getColumnValue(columnName);

                        // TODO : Because no attribute class is present, so
                        // cannot be done.
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
    public HTableInterface gethTable(final String tableName) throws IOException
    {
        return hTablePool.getTable(tableName);
    }

    /**
     * Puts HTable back into the HBase table pool.
     * 
     * @param hTable
     *            HBase Table instance
     */
    private void puthTable(HTableInterface hTable) throws IOException
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

            // Raw data retrieved from HBase for a particular row key (contains
            // all column families)

            Map<String, Object> relations = new HashMap<String, Object>();

            if (entity.getClass().isAssignableFrom(EnhanceEntity.class))
            {
                relations = ((EnhanceEntity) entity).getRelations();
                entity = ((EnhanceEntity) entity).getEntity();

            }

            // Populate columns data

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entityType = metaModel.entity(m.getEntityClazz());

            Set<Attribute> attributes = entityType.getAttributes();

            String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
            String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

            if (discrColumn != null && hbaseData.getColumnValue(discrColumn) != null && discrValue != null)
            {
                byte[] discrimnatorValue = hbaseData.getColumnValue(discrColumn);
                String actualDiscriminatorValue = Bytes.toString(discrimnatorValue);
                if (actualDiscriminatorValue != null && !actualDiscriminatorValue.equals(discrValue))
                {
                    entity = null;
                    return entity;
                }
            }

            for (Attribute attribute : attributes)
            {
                Class javaType = ((AbstractAttribute) attribute).getBindableJavaType();
                String key = ((AbstractAttribute) attribute).getJPAColumnName();
                if (metaModel.isEmbeddable(javaType))
                {
                    EmbeddableType columnFamily = metaModel.embeddable(javaType);

                    Field columnFamilyFieldInEntity = (Field) attribute.getJavaMember();
                    Class<?> columnFamilyClass = columnFamilyFieldInEntity.getType();

                    // Get a name->field map for columns in this column family
                    Map<String, Field> columnNameToFieldMap = MetadataUtils.createColumnsFieldMap(m, columnFamily);

                    // Column family can be either @Embedded or
                    // @EmbeddedCollection
                    if (Collection.class.isAssignableFrom(columnFamilyClass))
                    {
                        Map<Integer, Object> elementCollectionObjects = new HashMap<Integer, Object>();

                        Field embeddedCollectionField = (Field) attribute.getJavaMember();
                        Object[] embeddedObjectArr = new Object[hbaseData.getColumns().size()];
                        Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                        Map<String, byte[]> hbaseValues = hbaseData.getColumns();

                        Set<String> columnNames = hbaseValues.keySet();

                        for (String columnName : columnNames)
                        {
                            byte[] columnValue = hbaseData.getColumnValue(columnName);

                            if (columnName.startsWith(((AbstractAttribute) attribute).getJPAColumnName()))
                            {
                                String cfNamePostfix = MetadataUtils.getEmbeddedCollectionPostfix(columnName);
                                int cfNameCounter = Integer.parseInt(cfNamePostfix);
                                embeddedObject = elementCollectionObjects.get(cfNameCounter);

                                if (embeddedObject == null)
                                {

                                    embeddedObject = MetadataUtils
                                            .getEmbeddedGenericObjectInstance(embeddedCollectionField);
                                }

                                // Set Hbase data into the embedded object
                                setHBaseDataIntoObject(columnName, columnValue, columnNameToFieldMap, embeddedObject,
                                        metaModel.isEmbeddable(javaType));

                                elementCollectionObjects.put(cfNameCounter, embeddedObject);

                            }

                            if (relationNames != null && relationNames.contains(hbaseData.getColumnFamily())
                                    && columnValue.length != 0)
                            {
                                relations
                                        .put(hbaseData.getColumnFamily(),
                                                getObjectFromByteArray(entityType, columnValue,
                                                        hbaseData.getColumnFamily(), m));
                            }

                            // Save embedded object into Cache, needed while
                            // updation and deletion
                            ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(rowKey,
                                    embeddedObject, hbaseData.getColumnFamily());
                        }

                        for (Integer integer : elementCollectionObjects.keySet())
                        {
                            embeddedObjectArr[integer] = elementCollectionObjects.get(integer);
                        }

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
                        Object existsColumnFamilyObj = PropertyAccessorHelper.getObject(entity,
                                columnFamilyFieldInEntity);
                        Object columnFamilyObj = existsColumnFamilyObj != null ? existsColumnFamilyObj
                                : columnFamilyClass.newInstance();

                        Map<String, byte[]> hbaseValues = hbaseData.getColumns();
                        Set<String> columnNames = hbaseValues.keySet();

                        for (String columnName : columnNames)
                        {
                            byte[] columnValue = hbaseData.getColumnValue(columnName);
                            if (relationNames != null && relationNames.contains(hbaseData.getColumnFamily())
                                    && columnValue.length != 0)
                            {
                                relations
                                        .put(hbaseData.getColumnFamily(),
                                                getObjectFromByteArray(entityType, columnValue,
                                                        hbaseData.getColumnFamily(), m));
                            }
                            // Set Hbase data into the column family object
                            // Get Column from metadata
                            Field columnField = columnNameToFieldMap.get(columnName);
                            if (columnField != null && columnValue.length != 0)
                            {
                                if (columnFamilyFieldInEntity.isAnnotationPresent(Embedded.class)
                                        || columnFamilyFieldInEntity.isAnnotationPresent(EmbeddedId.class)
                                        || columnFamilyFieldInEntity.isAnnotationPresent(ElementCollection.class))
                                {
                                    PropertyAccessorHelper.set(columnFamilyObj, columnField,
                                            HBaseUtils.fromBytes(columnValue, columnField.getType()));
                                }
                                else
                                {
                                    columnFamilyObj = getObjectFromByteArray(entityType, columnValue,
                                            hbaseData.getColumnFamily(), m);
                                }
                            }
                        }

                        PropertyAccessorHelper.set(entity, columnFamilyFieldInEntity, columnFamilyObj);
                    }
                }
                else if (!attribute.getName().equals(m.getIdAttribute().getName()))
                {
                    Field columnField = (Field) attribute.getJavaMember();
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();

                    byte[] columnValue = hbaseData.getColumnValue(columnName);

                    if (relationNames != null && relationNames.contains(columnName) && columnValue != null
                            && columnValue.length > 0)
                    {
                        relations.put(columnName, getObjectFromByteArray(entityType, columnValue, columnName, m));
                    }
                    else if (columnValue != null && columnValue.length > 0)

                    {
                        PropertyAccessorHelper.set(entity, columnField,
                                HBaseUtils.fromBytes(columnValue, columnField.getType()));
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
    private void setHBaseDataIntoObject(String columnName, byte[] columnValue, Map<String, Field> columnNameToFieldMap,
            Object columnFamilyObj, boolean isEmbeddeble) throws PropertyAccessException
    {
        String qualifier = columnName.substring(columnName.indexOf("#") + 1, columnName.lastIndexOf("#"));

        // Get Column from metadata
        Field columnField = columnNameToFieldMap.get(qualifier);
        if (columnField != null)
        {
            if (isEmbeddeble)
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
    public void deleteRow(Object rowKey, String tableName, String columnFamilyName) throws IOException
    {
        hbaseWriter.delete(gethTable(tableName), rowKey, columnFamilyName);
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
        if (this.filter == null)
        {
            this.filter = new FilterList();
        }
        if (filter != null)
        {
            this.filter.addFilter(filter);
        }
    }

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
    private List onRead(String tableName, Class clazz, EntityMetadata m, List output, HTableInterface hTable,
            Object entity, List<String> relationNames, List<HBaseData> results) throws IOException
    {
        try
        {
            // Populate raw data from HBase into entity

            if (results != null)
            {
                Map<Object, Object> entityListMap = new HashMap<Object, Object>();

                for (HBaseData data : results)
                {
                    entity = KunderaCoreUtils.createNewInstance(clazz);
                    /* Set Row Key */

                    MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                            m.getPersistenceUnit());
                    Object rowKeyValue = HBaseUtils.fromBytes(m, metaModel, data.getRowKey());
                    if (!metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
                    {
                        PropertyAccessorHelper.setId(entity, m, rowKeyValue);
                    }

                    if (entityListMap.get(rowKeyValue) != null)
                    {

                        entity = entityListMap.get(rowKeyValue);

                    }
                    entity = populateEntityFromHbaseData(entity, data, m, null, relationNames);
                    if (output == null)
                    {
                        output = new ArrayList();
                    }
                    if (entity != null)
                    {
                        entityListMap.put(rowKeyValue, entity);

                    }
                }
                for (Object obj : entityListMap.values())
                {
                    output.add(obj);
                }
            }
        }
        catch (Exception e)
        {
            log.error("Error while creating an instance of {}, Caused by: .", clazz, e);
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
        private Object rowKey;

        // private Set<Attribute> columns;

        private Map<String, Attribute> columns;

        private Map<String, Object> values;

        private Object entity;

        private String columnFamily;

        /**
         * @param rowKey
         * @param columns
         * @param entity
         * @param columnFamily
         */
        public HBaseDataWrapper(Object rowKey, Map<String, Attribute> columns, Object entity, String columnFamily)
        {
            super();
            this.rowKey = rowKey;
            this.columns = columns;
            this.entity = entity;
            this.columnFamily = columnFamily;
        }

        public HBaseDataWrapper(Object rowKey, Map<String, Attribute> columns, Map<String, Object> values,
                Object entity, String columnFamily)
        {
            super();
            this.rowKey = rowKey;
            this.columns = columns;
            this.values = values;
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
        public Map<String, Attribute> getColumns()
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

        public void addColumn(String columnName, Attribute column)
        {
            columns.put(columnName, column);
        }

        public Map<String, Object> getValues()
        {
            return values;
        }

        public void addValue(String columnName, Object value)
        {
            if (values == null)
            {
                values = new HashMap<String, Object>();
            }
            values.put(columnName, value);
        }

        /**
         * @return the rowKey
         */
        public void setColumnFamily(String columnFamily)
        {
            this.columnFamily = columnFamily;
        }

        /**
         * @return the rowKey
         */
        public String getColumnFamily()
        {
            return this.columnFamily;
        }

    }

    public List scanData(Filter f, final String tableName, Class clazz, EntityMetadata m, String columnFamily,
            String qualifier) throws IOException, InstantiationException, IllegalAccessException
    {
        List returnedResults = new ArrayList();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();
        String[] columns = new String[attributes.size() - 1];
        int count = 0;
        boolean isCollection = false;
        for (Attribute attr : attributes)
        {
            if (!attr.isCollection() && !attr.getName().equalsIgnoreCase(m.getIdAttribute().getName()))
            {
                columns[count++] = ((AbstractAttribute) attr).getJPAColumnName();
            }
            else if (attr.isCollection())
            {
                isCollection = true;
                break;
            }
        }
        List<HBaseData> results = hbaseReader.loadAll(gethTable(tableName), f, null, null, m.getTableName(),
                isCollection ? qualifier : null, null);
        if (results != null)
        {
            for (HBaseData row : results)
            {
                Object entity = clazz.newInstance();// Entity Object
                /* Set Row Key */
                PropertyAccessorHelper.setId(entity, m, HBaseUtils.fromBytes(m, metaModel, row.getRowKey()));

                returnedResults.add(populateEntityFromHbaseData(entity, row, m, row.getRowKey(), m.getRelationNames()));
            }
        }
        return returnedResults;
    }

    @Override
    public Object[] scanRowyKeys(FilterList filterList, String tableName, String columnFamilyName, String columnName,
            final Class rowKeyClazz) throws IOException
    {
        HTableInterface hTable = null;
        hTable = gethTable(tableName);
        return hbaseReader.scanRowKeys(hTable, filterList, columnFamilyName, columnName, rowKeyClazz);
    }

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
                        (byte[]) value);
                return colValue;
            }
        }
        log.warn("No value found for column {}, returning null.", jpaColumnName);
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
    public Map<String, HBaseDataWrapper> preparePersistentData(String tableName, String columnFamily, Object entity,
            Object rowId, MetamodelImpl metaModel, Set<Attribute> attributes, HBaseDataWrapper columnWrapper,
            List<HBaseDataWrapper> persistentData, boolean showQuery) throws IOException
    {

        Map<String, HBaseDataWrapper> persistentDataWrappers = new HashMap<String, HBaseDataWrapper>();
        persistentDataWrappers.put(columnFamily, columnWrapper);
        StringBuilder printQuery = null;
        if (showQuery)
        {
            printQuery = new StringBuilder("Persist data into ").append(columnFamily).append(" with PK=")
                    .append(rowId.toString()).append(" , ");
        }

        for (Attribute column : attributes)
        {
            String fieldName = ((AbstractAttribute) column).getJPAColumnName();
            String columFamilyTableName = ((AbstractAttribute) column).getTableName() != null ? ((AbstractAttribute) column)
                    .getTableName() : columnFamily;

            persistentDataWrappers = getHBaseWrapperObj(rowId, entity, columFamilyTableName, persistentDataWrappers,
                    persistentData);

            columnWrapper = persistentDataWrappers.get(columFamilyTableName);
            columnWrapper.setColumnFamily(columFamilyTableName);

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
                catch (PropertyAccessException paex)
                {
                    log.error("Error while getting {}, field from entity {} .", columnFamilyName, entity);
                    throw new KunderaException(paex);
                }

                if (columnFamilyObject != null)
                {
                    // continue;
                    Set<Attribute> columns = metaModel.embeddable(javaType).getAttributes();

                    Map<String, Attribute> columnNameToAttribute = new HashMap<String, Attribute>();
                    Map<String, Object> columnNameToValue = new HashMap<String, Object>();
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
                                String embeddableColumFamilyName = columFamilyTableName;
                                Map<Map<String, Class<?>>, HBaseDataWrapper> embeDaableDataWrappers = new HashMap<Map<String, Class<?>>, HBaseDataWrapper>();
                                for (Attribute attribute : columns)
                                {
                                    embeddableColumFamilyName = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                                            .getTableName() : columFamilyTableName;
                                    String columnName = columnFamilyName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                            + ((AbstractAttribute) attribute).getJPAColumnName()
                                            + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                                    Map<String, Class<?>> embeddableMap = new HashMap<String, Class<?>>();
                                    embeddableMap.put(embeddableColumFamilyName, obj.getClass());

                                    embeDaableDataWrappers = getEmbeddableHBaseWrapperObj(rowId, obj,
                                            embeddableColumFamilyName, embeDaableDataWrappers, persistentData,
                                            embeddableMap);

                                    HBaseDataWrapper embeddableColumnWrapper = embeDaableDataWrappers
                                            .get(embeddableMap);
                                    embeddableColumnWrapper.setColumnFamily(embeddableColumFamilyName);

                                    embeddableColumnWrapper.addColumn(columnName, attribute);
                                    embeddableColumnWrapper.addValue(columnName,
                                            PropertyAccessorHelper.getObject(obj, (Field) attribute.getJavaMember()));
                                }
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
                                String embeddableColumFamilyName = columFamilyTableName;
                                dynamicCFName = ecCacheHandler.getElementCollectionObjectName(rowId, obj);
                                if (dynamicCFName == null)
                                { // Fresh row
                                    ++lastEmbeddedObjectCount;
                                    Map<Map<String, Class<?>>, HBaseDataWrapper> embeDaableDataWrappers = new HashMap<Map<String, Class<?>>, HBaseDataWrapper>();
                                    for (Attribute attribute : columns)
                                    {
                                        embeddableColumFamilyName = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                                                .getTableName() : columFamilyTableName;
                                        String columnName = columnFamilyName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                                + ((AbstractAttribute) attribute).getJPAColumnName()
                                                + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + (lastEmbeddedObjectCount);

                                        Map<String, Class<?>> embeddableMap = new HashMap<String, Class<?>>();
                                        embeddableMap.put(embeddableColumFamilyName, obj.getClass());

                                        embeDaableDataWrappers = getEmbeddableHBaseWrapperObj(rowId, obj,
                                                embeddableColumFamilyName, embeDaableDataWrappers, persistentData,
                                                embeddableMap);

                                        HBaseDataWrapper embeddableColumnWrapper = embeDaableDataWrappers
                                                .get(embeddableMap);
                                        embeddableColumnWrapper.setColumnFamily(embeddableColumFamilyName);

                                        embeddableColumnWrapper.addColumn(columnName, attribute);
                                        embeddableColumnWrapper.addValue(columnName, PropertyAccessorHelper.getObject(
                                                obj, (Field) attribute.getJavaMember()));
                                    }
                                }
                            }
                            // Clear embedded collection cache for GC
                            ecCacheHandler.clearCache();
                        }
                    }
                    else
                    {

                        // Write Column family which was Embedded object in
                        // entity
                        Map<Map<String, Class<?>>, HBaseDataWrapper> embeDaableDataWrappers = new HashMap<Map<String, Class<?>>, HBaseDataWrapper>();
                        String embeddableColumFamilyName = columFamilyTableName;
                        for (Attribute attribute : columns)
                        {
                            embeddableColumFamilyName = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                                    .getTableName() : columFamilyTableName;

                            Map<String, Class<?>> embeddableMap = new HashMap<String, Class<?>>();
                            embeddableMap.put(embeddableColumFamilyName, columnFamilyObject.getClass());

                            embeDaableDataWrappers = getEmbeddableHBaseWrapperObj(rowId, columnFamilyObject,
                                    embeddableColumFamilyName, embeDaableDataWrappers, persistentData, embeddableMap);

                            HBaseDataWrapper embeddableColumnWrapper = embeDaableDataWrappers.get(embeddableMap);

                            embeddableColumnWrapper.setColumnFamily(embeddableColumFamilyName);

                            embeddableColumnWrapper.addColumn(((AbstractAttribute) attribute).getJPAColumnName(),
                                    attribute);
                            embeddableColumnWrapper.addValue(
                                    ((AbstractAttribute) attribute).getJPAColumnName(),
                                    PropertyAccessorHelper.getObject(columnFamilyObject,
                                            (Field) attribute.getJavaMember()));
                        }
                    }
                }
            }
            else if (!column.isAssociation())
            {
                Object fieldValue = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
                columnWrapper.addColumn(fieldName, column);
                columnWrapper.addValue(fieldName, fieldValue);
                if (showQuery)
                {
                    fieldValue = fieldValue != null ? fieldValue.toString() : fieldValue;
                    printQuery.append(fieldName).append("=").append(fieldValue).append(" , ");
                }
            }
        }

        if (showQuery)
        {
            KunderaCoreUtils.printQuery(printQuery.substring(0, printQuery.lastIndexOf(" , ")).toString(), showQuery);
        }

        return persistentDataWrappers;
    }

    private Map<String, HBaseDataWrapper> getHBaseWrapperObj(Object rowKey, Object entity, String columnFamily,
            Map<String, HBaseDataWrapper> persistentDataMap, List<HBaseDataWrapper> persistentData)
    {

        HBaseDataWrapper existsHbaseWrapper = persistentDataMap.get(columnFamily);

        if (existsHbaseWrapper == null)
        {
            HBaseDataWrapper hbaseWrapper = new HBaseDataWrapper(rowKey, new java.util.HashMap<String, Attribute>(),
                    entity, columnFamily);
            persistentDataMap.put(columnFamily, hbaseWrapper);
            persistentData.add(hbaseWrapper);

        }
        return persistentDataMap;
    }

    private Map<Map<String, Class<?>>, HBaseDataWrapper> getEmbeddableHBaseWrapperObj(Object rowKey, Object entity,
            String columnFamily, Map<Map<String, Class<?>>, HBaseDataWrapper> persistentDataMap,
            List<HBaseDataWrapper> persistentData, Map<String, Class<?>> embeddableMap)
    {
        HBaseDataWrapper existsHbaseWrapper = persistentDataMap.get(embeddableMap);

        if (existsHbaseWrapper == null)
        {
            HBaseDataWrapper hbaseWrapper = new HBaseDataWrapper(rowKey, new java.util.HashMap<String, Attribute>(),
                    entity, columnFamily);
            persistentDataMap.put(embeddableMap, hbaseWrapper);
            persistentData.add(hbaseWrapper);
        }
        return persistentDataMap;
    }

    /**
     * @param data
     * @throws IOException
     */
    public void batch_insert(Map<HTableInterface, List<HBaseDataWrapper>> data) throws IOException
    {
        hbaseWriter.persistRows(data);
    }

    public void setFetchSize(final int fetchSize)
    {
        ((HBaseReader) hbaseReader).setFetchSize(fetchSize);
    }

    public Object next(EntityMetadata m)
    {
        Object entity = null;
        HBaseData result = ((HBaseReader) hbaseReader).next();
        List<HBaseData> results = new ArrayList<HBaseData>();
        List output = new ArrayList();
        results.add(result);
        try
        {
            output = onRead(m.getSchema(), m.getEntityClazz(), m, output, gethTable(m.getSchema()), entity,
                    m.getRelationNames(), results);
        }
        catch (IOException e)
        {
            log.error("Error during finding next record, Caused by: .", e);
            throw new KunderaException(e);
        }

        return output != null && !output.isEmpty() ? output.get(0) : output;
    }

    public boolean hasNext()
    {
        return ((HBaseReader) hbaseReader).hasNext();
    }

    public void reset()
    {
        resetFilter();
        ((HBaseReader) hbaseReader).reset();
    }

    public void resetFilter()
    {
        filter = null;
        filters = new ConcurrentHashMap<String, FilterList>();
    }

    public HBaseDataHandler getHandle()
    {
        HBaseDataHandler handler = new HBaseDataHandler(this.kunderaMetadata, this.conf, this.hTablePool);
        handler.filter = this.filter;
        handler.filters = this.filters;
        return handler;
    }

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
}