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
package com.impetus.client.hbase.schemamanager;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.config.HBaseColumnFamilyProperties;
import com.impetus.client.hbase.config.HBasePropertyReader;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.Table;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;

/**
 * Manages auto schema operation {@code ScheamOperationType} for HBase data
 * store.
 * 
 * @author Kuldeep.kumar
 * 
 */
public class HBaseSchemaManager extends AbstractSchemaManager implements SchemaManager
{
    /**
     * Hbase admin variable holds the admin authorities.
     */
    private static HBaseAdmin admin;

    /**
     * logger used for logging statement.
     */
    private static final Logger logger = LoggerFactory.getLogger(HBaseSchemaManager.class);

    // private Properties schemaProperties;

    private List<Table> tables;

    /**
     * Initialises HBase schema manager
     * 
     * @param clientFactory
     *            client factory.
     */
    public HBaseSchemaManager(String clientFactory)
    {
        super(clientFactory);
    }

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema()
    {
        super.exportSchema();
    }

    /**
     * update method update schema and table for the list of tableInfos
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void update(List<TableInfo> tableInfos)
    {
        for (TableInfo tableInfo : tableInfos)
        {
            if (tableInfo != null && tableInfo.getTableName() != null)
            {
                HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
                try
                {
                    HTableDescriptor descriptor = admin.getTableDescriptor(tableInfo.getTableName().getBytes());
                    if (descriptor.getNameAsString().equalsIgnoreCase(tableInfo.getTableName()))
                    {
                        if (admin.isTableEnabled(tableInfo.getTableName().getBytes()))
                        {
                            admin.disableTable(tableInfo.getTableName().getBytes());
                        }
                        HColumnDescriptor[] descriptors = descriptor.getColumnFamilies();
                        if (tableInfo.getColumnMetadatas() != null)
                        {
                            for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                            {
                                boolean found = false;
                                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnInfo.getColumnName());
                                for (HColumnDescriptor hColumnDescriptor : descriptors)
                                {
                                    if (hColumnDescriptor.getNameAsString()
                                            .equalsIgnoreCase(columnInfo.getColumnName()))
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    admin.addColumn(tableInfo.getTableName(), columnDescriptor);
                                }
                            }
                        }
                        if (tableInfo.getEmbeddedColumnMetadatas() != null)
                        {
                            for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
                            {
                                boolean found = false;
                                HColumnDescriptor columnDescriptor = new HColumnDescriptor(
                                        embeddedColumnInfo.getEmbeddedColumnName());
                                for (HColumnDescriptor hColumnDescriptor : descriptors)
                                {
                                    if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(
                                            embeddedColumnInfo.getEmbeddedColumnName()))
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    admin.addColumn(tableInfo.getTableName(), columnDescriptor);
                                }
                            }
                        }
                    }
                }
                catch (IOException e)
                {
                    try
                    {
                        admin.createTable(hTableDescriptor);
                    }
                    catch (IOException e1)
                    {
                        logger.error("Check for network connection, Caused by:" + e.getMessage());
                        throw new SchemaGenerationException(e, "Hbase");
                    }

                }
            }
        }
    }

    /**
     * validate method validate schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void validate(List<TableInfo> tableInfos)
    {
        for (TableInfo tableInfo : tableInfos)
        {
            HTableDescriptor hTableDescriptor;
            try
            {
                hTableDescriptor = admin.getTableDescriptor(tableInfo.getTableName().getBytes());
                if (tableInfo.getColumnMetadatas() != null)
                {
                    for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                    {
                        boolean isColumnFound = false;
                        for (HColumnDescriptor columnDescriptor : hTableDescriptor.getColumnFamilies())
                        {
                            if (columnDescriptor.getNameAsString().equalsIgnoreCase(columnInfo.getColumnName()))
                            {
                                isColumnFound = true;
                                break;
                            }
                        }
                        if (!isColumnFound)
                        {
                            throw new SchemaGenerationException("column " + columnInfo.getColumnName()
                                    + " does not exist in table " + tableInfo.getTableName() + "", "Hbase",
                                    tableInfo.getTableName(), tableInfo.getTableName());
                        }
                    }
                }
                if (tableInfo.getEmbeddedColumnMetadatas() != null)
                {
                    for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
                    {
                        boolean isColumnFound = false;
                        for (HColumnDescriptor columnDescriptor : hTableDescriptor.getColumnFamilies())
                        {
                            if (columnDescriptor.getNameAsString().equalsIgnoreCase(
                                    embeddedColumnInfo.getEmbeddedColumnName()))
                            {
                                isColumnFound = true;
                                break;
                            }
                        }
                        if (!isColumnFound)
                        {
                            throw new SchemaGenerationException("column " + embeddedColumnInfo.getEmbeddedColumnName()
                                    + " does not exist in table " + tableInfo.getTableName() + "", "Hbase",
                                    tableInfo.getTableName(), tableInfo.getTableName());
                        }
                    }
                }
            }
            catch (TableNotFoundException tnfex)
            {
                throw new SchemaGenerationException("table " + tableInfo.getTableName() + " does not exist ", tnfex,
                        "Hbase");
            }
            catch (IOException e)
            {
                logger.error("Either check for network connection or table isn't in enabled state, Caused by:"
                        + e.getMessage());
                throw new SchemaGenerationException(e, "Hbase");
            }
        }
    }

    /**
     * create_drop method creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create_drop(List<TableInfo> tableInfos)
    {
        create(tableInfos);
    }

    /**
     * create method creates schema and table for the list of tableInfos.
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void create(List<TableInfo> tableInfos)
    {
        for (TableInfo tableInfo : tableInfos)
        {
            if (tableInfo != null && tableInfo.getTableName() != null)
            {
                try
                {
                    admin.getTableDescriptor(tableInfo.getTableName().getBytes());
                    admin.disableTable(tableInfo.getTableName());
                    admin.deleteTable(tableInfo.getTableName());
                }
                catch (TableNotFoundException e)
                {
                    logger.info("creating table " + tableInfo.getTableName());
                }
                catch (IOException ioex)
                {
                    logger.error("Either table isn't in enabled state or some network problem, Caused by: "
                            + ioex.getMessage());
                    throw new SchemaGenerationException(ioex, "Hbase");
                }
                HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
                try
                {
                    admin.createTable(hTableDescriptor);
                }
                catch (IOException ioex1)
                {
                    logger.error("Table isn't in enabled state, Caused by:" + ioex1.getMessage());
                    throw new SchemaGenerationException(ioex1, "Hbase");
                }
            }
        }
    }

    /**
     * drop schema method drop the table
     */
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            for (TableInfo tableInfo : tableInfos)
            {
                if (tableInfo != null && tableInfo.getTableName() != null)
                {
                    try
                    {
                        admin.disableTable(tableInfo.getTableName());
                        admin.deleteTable(tableInfo.getTableName());
                    }
                    catch (TableNotFoundException e)
                    {
                        logger.error("Table doesn't exist, Caused by " + e.getMessage());
                        throw new SchemaGenerationException(e, "Hbase");
                    }
                    catch (IOException e)
                    {
                        logger.error("Table isn't in enabled state, Caused by" + e.getMessage());
                        throw new SchemaGenerationException(e, "Hbase");
                    }
                }
            }
        }
        operation = null;
        admin = null;
    }

    /**
     * initiate client method initiates the client.
     * 
     * @return boolean value ie client started or not.
     * 
     */
    protected boolean initiateClient()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", host + ":" + port);
        conn = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getConnection() : null;
        if (conn != null && conn.getProperties() != null)
        {
            String zookeeperHost = conn.getProperties().getProperty("hbase.zookeeper.quorum");
            String zookeeperPort = conn.getProperties().getProperty("hbase.zookeeper.property.clientPort");
            hadoopConf.set("hbase.zookeeper.quorum", zookeeperHost != null ? zookeeperHost : host);
            hadoopConf.set("hbase.zookeeper.property.clientPort", zookeeperPort != null ? zookeeperPort : "2181");
        }
        else
        {
            hadoopConf.set("hbase.zookeeper.quorum", HBasePropertyReader.hsmd.getZookeeperHost());
            hadoopConf.set("hbase.zookeeper.property.clientPort", HBasePropertyReader.hsmd.getZookeeperPort());

        }
        HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
        try
        {
            admin = new HBaseAdmin(conf);
        }
        catch (MasterNotRunningException e)
        {
            logger.error("Master not running exception, Caused by:" + e.getMessage());
            throw new SchemaGenerationException(e, "Hbase");
        }
        catch (ZooKeeperConnectionException e)
        {
            logger.equals("Unable to connect to zookeeper, Caused by:" + e.getMessage());
            throw new SchemaGenerationException(e, "Hbase");
        }
        return true;
    }

    /**
     * get Table metadata method returns the HTableDescriptor of table for given
     * tableInfo
     * 
     * @return HHTableDescriptor object for tableInfo
     */
    private HTableDescriptor getTableMetaData(TableInfo tableInfo)
    {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableInfo.getTableName());
        Properties tableProperties = null;
        schemas = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getSchemas() : null;
        if (schemas != null && !schemas.isEmpty())
        {
            for (Schema s : schemas)
            {
                if (s.getName() != null && s.getName().equalsIgnoreCase(tableInfo.getTableName()))
                {
                    tableProperties = s.getSchemaProperties();
                    tables = s.getTables();
                }
            }
        }
        if (tableInfo.getColumnMetadatas() != null)
        {
            for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnInfo.getColumnName());
                setColumnFamilyProperties(hColumnDescriptor, tableInfo.getTableName());
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
        }
        if (tableInfo.getEmbeddedColumnMetadatas() != null)
        {
            for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(embeddedColumnInfo.getEmbeddedColumnName());
                setColumnFamilyProperties(hColumnDescriptor, tableInfo.getTableName());
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
        }

        if (tableProperties != null)
        {
            for (Object o : tableProperties.keySet())
            {
                hTableDescriptor
                        .setValue(Bytes.toBytes(o.toString()), Bytes.toBytes(tableProperties.get(o).toString()));
            }
        }
        return hTableDescriptor;
    }

    private void setColumnFamilyProperties(HColumnDescriptor hColumnDescriptor, String tableName)
    {
        dataStore = HBasePropertyReader.hsmd.getDataStore();
        if (dataStore != null)
        {
            if (tables != null && !tables.isEmpty())
            {
                for (Table t : tables)
                {
                    Properties columnProperties = t.getProperties();
                    if (t.getName() != null && t.getName().equalsIgnoreCase(hColumnDescriptor.getNameAsString())
                            && columnProperties != null)
                    {
                        for (Object o : columnProperties.keySet())
                        {
                            hColumnDescriptor.setValue(Bytes.toBytes(o.toString()),
                                    Bytes.toBytes(columnProperties.get(o).toString()));
                        }
                    }
                }
            }
        }
        else if (HBasePropertyReader.hsmd.getColumnFamilyProperties().containsKey(tableName))
        {
            HBaseColumnFamilyProperties familyProperties = HBasePropertyReader.hsmd.getColumnFamilyProperties().get(
                    tableName);
            hColumnDescriptor.setTimeToLive(familyProperties.getTtl());
            hColumnDescriptor.setMaxVersions(familyProperties.getMaxVersion());
            hColumnDescriptor.setMinVersions(familyProperties.getMinVersion());
            hColumnDescriptor.setCompactionCompressionType(familyProperties.getAlgorithm());
            hColumnDescriptor.setCompressionType(familyProperties.getAlgorithm());
        }

    }

    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return true;
    }

}
