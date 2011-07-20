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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.property.PropertyAccessException;
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
    public HBaseData readData(final String tableName, final String columnFamily, final String[] columnName,
            final String rowKey) throws IOException
    {
        return hbaseReader.LoadData(gethTable(tableName), columnFamily, columnName, rowKey);
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
            List<Column> columns = columnFamily.getColumns();
            
            //TODO: Handle Embedded collections differently
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
                }         
                
                
            } else {       
                
                Object columnFamilyObj = null;
                try
                {
                    columnFamilyObj = PropertyAccessorHelper.getObject(e.getEntity(), columnFamilyName);
                }
                catch (PropertyAccessException e2)
                {
                    log.error("Can't fetch column family object " + columnFamily + " from entity");
                    return;
                }               
                
                hbaseWriter.writeColumns(gethTable(tableName), columnFamilyName, e.getId(), columns, columnFamilyObj);
            }
            
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

}
