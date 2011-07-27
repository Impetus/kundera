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
package com.impetus.kundera.hbase.admin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.Constants;
import com.impetus.kundera.hbase.client.HBaseData;
import com.impetus.kundera.hbase.client.Reader;
import com.impetus.kundera.hbase.client.Writer;
import com.impetus.kundera.hbase.client.service.HBaseReader;
import com.impetus.kundera.hbase.client.service.HBaseWriter;
import com.impetus.kundera.metadata.EmbeddedCollectionCacheHandler;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.Relation;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author impetus
 */
public class HBaseDataHandler implements DataHandler
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseDataHandler.class);

    private HBaseConfiguration conf;

    private HBaseAdmin admin;

    private Reader hbaseReader = new HBaseReader();

    private Writer hbaseWriter = new HBaseWriter();

    public HBaseDataHandler(String hostName, String port)
    {
        try
        {
            init(hostName, port);
        }
        catch (MasterNotRunningException e)
        {
            throw new RuntimeException(e.getMessage());
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
            if(admin.tableExists(tableName)) {
                
                //Before any modification to table schema, it's necessary to disable it                
                if(admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }                
                admin.addColumn(tableName, cfDesciptor);
                
                //Enable table once done
                admin.enableTable(tableName);
            } else {
                log.warn("Table " + tableName + " doesn't exist, so no question of adding column family " + columnFamilyName + " to it!");
            }                                
        }
        catch (IOException e)
        {
            log.error("Error while adding column family " + columnFamilyName + " to table " + tableName);
            throw e;
        }
        
    }

    @Override
    public <E> E readData(final String tableName, Class<E> clazz, EntityMetadata m, final String rowKey) throws IOException
    {        
        
        E e = null;
        try
        {
            e = clazz.newInstance();    
            
            //Load raw data from HBase 
            HBaseData data = hbaseReader.LoadData(gethTable(tableName), rowKey);                          
            
            //Populate raw data from HBase into entity
            populateEntityFromHbaseData(e, data, m, rowKey);           
        }
        catch (InstantiationException e1)
        {
            log.error("Error while creating an instance of " + clazz);
            return e;
        }
        catch (IllegalAccessException e1)
        {
            log.error("Illegal Access while reading data from " + tableName + ";Details: " + e1.getMessage());
            return e;
        }
        return e;        
    }

    @Override
    public void writeData(String tableName, EntityMetadata m, EnhancedEntity e)
            throws IOException
    {        
        
        //Now persist column families in the table
        List<SuperColumn> columnFamilies = m.getSuperColumnsAsList();  //Yes, for HBase they are called column families
        for(SuperColumn columnFamily : columnFamilies) {
            String columnFamilyName = columnFamily.getName();
            Field columnFamilyField = columnFamily.getField();            
            Object columnFamilyObject = null;
            try
            {
                columnFamilyObject = PropertyAccessorHelper.getObject(e.getEntity(), columnFamilyField);
            }
            catch (PropertyAccessException e1)
            {
                log.error("Error while getting " + columnFamilyName + " field from entity " + e.getEntity());
                return;
            }
            
            if(columnFamilyObject == null) {
                return;
            }
            
            List<Column> columns = columnFamily.getColumns();
            
            //TODO: Handle Embedded collections differently
            //Write Column family which was Embedded collection in entity
            if(columnFamilyObject instanceof Collection) {
                String dynamicCFName = null;        
                
                EmbeddedCollectionCacheHandler ecCacheHandler = m.getEcCacheHandler();
                // Check whether it's first time insert or updation
                if (ecCacheHandler.isCacheEmpty())
                { // First time insert
                    int count = 0;
                    for (Object obj : (Collection) columnFamilyObject)
                    {
                        dynamicCFName = columnFamilyName + Constants.SUPER_COLUMN_NAME_DELIMITER + count;
                        addColumnFamilyToTable(tableName, dynamicCFName);
                        
                        hbaseWriter.writeColumns(gethTable(tableName), dynamicCFName, e.getId(), columns, obj);
                        count++;
                    }
                    
                } else {
                    // Updation
                    // Check whether this object is already in cache, which
                    // means we already have a column family with that name
                    // Otherwise we need to generate a fresh column family name
                    int lastEmbeddedObjectCount = ecCacheHandler.getLastEmbeddedObjectCount(e.getId());                    
                    for (Object obj : (Collection) columnFamilyObject)
                    {
                        dynamicCFName = ecCacheHandler.getEmbeddedObjectName(e.getId(), obj);
                        if (dynamicCFName == null)
                        { // Fresh row
                            dynamicCFName = columnFamilyName + Constants.SUPER_COLUMN_NAME_DELIMITER
                                    + (++lastEmbeddedObjectCount);
                        }
                    }
                    
                    //Clear embedded collection cache for GC
                    ecCacheHandler.clearCache();
                }                
                
            } else {
                //Write Column family which was Embedded object in entity
                hbaseWriter.writeColumns(gethTable(tableName), columnFamilyName, e.getId(), columns, columnFamilyObject);
            }
            
        }  
        
        
        //HBase tables may have columns alongwith column families
        List<Column> columns = m.getColumnsAsList();    
        if(columns != null && ! columns.isEmpty()) {
            hbaseWriter.writeColumns(gethTable(tableName), e.getId(), columns, e.getEntity());
        }
        
        //Persist relationships as a column in newly created Column family by Kundera
        List<Relation> relations = m.getRelations();
        for (Map.Entry<String, Set<String>> entry : e.getForeignKeysMap().entrySet())
        {
            String property = entry.getKey();
            Set<String> foreignKeys = entry.getValue();

            String keys = MetadataUtils.serializeKeys(foreignKeys);
            if (null != keys)
            {   //EntityMetadata.Column col = new EntityMetadata().Column(property, null);


                List<Column> columns2 = new ArrayList<EntityMetadata.Column>();
                //columns.add(col);
            }
            hbaseWriter.writeColumns(gethTable(tableName), Constants.TO_ONE_SUPER_COL_NAME, e.getId(), columns, keys);
        }
        
                
    }    


    private void loadConfiguration(final String hostName, final String port) throws MasterNotRunningException
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", hostName + ":" + port);
        conf = new HBaseConfiguration(hadoopConf);
        getHBaseAdmin();
    }

    /**
     * 
     * @param hostName
     * @param port
     * @throws MasterNotRunningException
     */
    private void init(final String hostName, final String port) throws MasterNotRunningException
    {
        if (conf == null)
        {
            loadConfiguration(hostName, port);
        }
    }

    /**
     * @throws MasterNotRunningException
     */
    private void getHBaseAdmin() throws MasterNotRunningException
    {
        admin = new HBaseAdmin(conf);
    }

    private HTable gethTable(final String tableName) throws IOException
    {
        return new HTable(conf, tableName);
    }

    @Override
    public void shutdown()
    {
        try
        {
            admin.shutdown();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }
    
    //TODO: Scope of performance improvement in this method
    private void populateEntityFromHbaseData(Object entity, HBaseData data, EntityMetadata m, String rowKey)
    {       
        try
        {  
            /*Set Row Key*/
            PropertyAccessorHelper.set(entity, m.getIdProperty(), rowKey);  
            
            /*Set each column families*/
            List<SuperColumn> columnFamilies = m.getSuperColumnsAsList();  //Yes, for HBase they are called column families
            for(SuperColumn columnFamily : columnFamilies) {            
                Field columnFamilyFieldInEntity = columnFamily.getField();
                Class<?> columnFamilyClass = columnFamilyFieldInEntity.getType();
                
                //Get a name->field map for columns in this column family
                Map<String, Field> columnNameToFieldMap = MetadataUtils.createColumnsFieldMap(m, columnFamily);  
                
                //Raw data retrieved from HBase for a particular row key (contains all column families)
                List<KeyValue> hbaseValues = data.getColumns();  
                
                //Column family can be either @Embedded or @EmbeddedCollection
                if(Collection.class.isAssignableFrom(columnFamilyClass)) {
                    
                    Field embeddedCollectionField = columnFamily.getField();                                    
                    Object[] embeddedObjectArr = new Object[hbaseValues.size()];  //Array to hold column family objects
                    
                    
                    Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);
                    int prevCFNameCounter = 0;    //Previous CF name counter
                    for (KeyValue colData : hbaseValues)
                    {
                        String cfInHbase = Bytes.toString(colData.getFamily());                   
                        //Only populate those data from Hbase into entity that matches with column family name 
                        // in the format <Collection field name>#<sequence count>
                        if(! cfInHbase.startsWith(columnFamily.getName())) {
                            continue;
                        }
                        
                        String cfNamePostfix = MetadataUtils.getEmbeddedCollectionPostfix(cfInHbase);                        
                        int cfNameCounter = Integer.parseInt(cfNamePostfix);
                        if(cfNameCounter != prevCFNameCounter) {
                            prevCFNameCounter = cfNameCounter;
                            
                            //Fresh embedded object for the next column family in collection
                            embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);
                        }                        
                        
                        //Set Hbase data into the embedded object
                        setHBaseDataIntoObject(colData, columnNameToFieldMap, embeddedObject);              
                        
                        embeddedObjectArr[cfNameCounter] = embeddedObject;                        
                        
                        //Save embedded object into Cache, needed while updation and deletion
                        m.getEcCacheHandler().addEmbeddedCollectionCacheMapping(rowKey, embeddedObject, cfInHbase);
                    }             
                    
                    //Collection to hold column family objects
                    Collection embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                    embeddedCollection.addAll(Arrays.asList(embeddedObjectArr));
                    embeddedCollection.removeAll(Collections.singletonList(null));
                    embeddedObjectArr = null;    //Eligible for GC
                    
                    //Now, set the embedded collection into entity
                    if(embeddedCollection != null && ! embeddedCollection.isEmpty()) {
                        PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
                    }
                    
                } else {
                    Object columnFamilyObj = columnFamilyClass.newInstance();                                      
                    
                    for (KeyValue colData : hbaseValues)
                    {
                        String cfInHbase = Bytes.toString(colData.getFamily());
                        
                        if(!cfInHbase.equals(columnFamily.getName())) {
                            continue;
                        }                        
                        
                        //Set Hbase data into the column family object
                        setHBaseDataIntoObject(colData, columnNameToFieldMap, columnFamilyObj);                                
                    }
                    PropertyAccessorHelper.set(entity, columnFamilyFieldInEntity, columnFamilyObj);   
                }               
                             
            }         
            
        }        
        catch (PropertyAccessException e1)
        {
            throw new RuntimeException(e1.getMessage());
        } 
        catch(InstantiationException e1) 
        {
            throw new RuntimeException(e1.getMessage());
        }
        catch(IllegalAccessException e1) 
        {
            throw new RuntimeException(e1.getMessage());
        }
    }
    
    private void setHBaseDataIntoObject(KeyValue colData, Map<String, Field> columnNameToFieldMap, Object columnFamilyObj) 
        throws PropertyAccessException {      
        
        String colName = Bytes.toString(colData.getQualifier());
        byte[] columnValue = colData.getValue();                 

        // Get Column from metadata
        Field columnField = columnNameToFieldMap.get(colName);
        if(columnField != null) {
            PropertyAccessorHelper.set(columnFamilyObj, columnField, columnValue);
        }                        
    }

}
