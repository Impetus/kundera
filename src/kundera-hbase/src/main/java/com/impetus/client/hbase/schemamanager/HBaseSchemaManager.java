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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
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

import com.impetus.client.hbase.config.HBasePropertyReader;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.Table;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * Manages auto schema operation {@code ScheamOperationType} for HBase data
 * store.
 * 
 * @author Kuldeep.kumar
 * 
 */
public class HBaseSchemaManager extends AbstractSchemaManager implements SchemaManager
{
    private static final String DEFAULT_ZOOKEEPER_PORT = "2181";

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
    public HBaseSchemaManager(String clientFactory, Map<String, Object> puProperties, final KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, puProperties, kunderaMetadata);
    }

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
    }

    /**
     * update method update schema and table for the list of tableInfos
     * 
     * @param tableInfos
     *            list of TableInfos.
     */
    protected void update(List<TableInfo> tableInfos)
    {
        try
        {
            if (admin.isTableAvailable(databaseName))
            {
                HTableDescriptor hTableDescriptor = admin.getTableDescriptor(databaseName.getBytes());
                addColumnFamilies(tableInfos, hTableDescriptor, true);
            }
            else
            {
                createTable(tableInfos);
            }
        }
        catch (IOException ioe)
        {
            logger.error("Either check for network connection or table isn't in enabled state, Caused by:", ioe);
            throw new SchemaGenerationException(ioe, "Hbase");
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
        try
        {
            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(databaseName.getBytes());
            HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
            for (TableInfo tableInfo : tableInfos)
            {
                if (tableInfo != null)
                {
                    boolean isColumnFound = false;
                    for (HColumnDescriptor columnDescriptor : columnFamilies)
                    {
                        if (columnDescriptor.getNameAsString().equalsIgnoreCase(tableInfo.getTableName()))
                        {
                            isColumnFound = true;
                            break;
                        }
                    }
                    if (!isColumnFound)
                    {
                        throw new SchemaGenerationException("column " + tableInfo.getTableName()
                                + " does not exist in table " + databaseName + "", "Hbase", databaseName,
                                tableInfo.getTableName());
                    }
                }
            }
        }
        catch (TableNotFoundException tnfex)
        {
            throw new SchemaGenerationException("table " + databaseName + " does not exist ", tnfex, "Hbase");
        }
        catch (IOException ioe)
        {
            logger.error("Either check for network connection or table isn't in enabled state, Caused by:", ioe);
            throw new SchemaGenerationException(ioe, "Hbase");
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
        try
        {
            if (admin.isTableAvailable(databaseName))
            {
                addColumnFamilies(tableInfos, admin.getTableDescriptor(databaseName.getBytes()), false);
            }
            else
            {
                createTable(tableInfos);
            }
            if (admin.isTableDisabled(databaseName))
            {
                admin.enableTable(databaseName);
            }
        }
        catch (TableNotFoundException e)
        {
            logger.info("creating table " + databaseName);
        }
        catch (IOException ioex)
        {
            logger.error("Either table isn't in enabled state or some network problem, Caused by: ", ioex);
            throw new SchemaGenerationException(ioex, "Hbase");
        }
    }

    private void addColumnFamilies(List<TableInfo> tableInfos, HTableDescriptor hTableDescriptor, boolean isUpdate)
            throws IOException
    {
        if (admin.isTableEnabled(databaseName))
        {
            admin.disableTable(databaseName);
        }
        for (TableInfo tableInfo : tableInfos)
        {
            HColumnDescriptor columnDescriptor = hTableDescriptor.getFamily(tableInfo.getTableName().getBytes());
            if (columnDescriptor != null && !isUpdate)
            {
                admin.deleteColumn(databaseName, tableInfo.getTableName());
                addColumn(tableInfo);
            }
            else if (columnDescriptor == null)
            {
                addColumn(tableInfo);
            }
        }

        if (admin.isTableDisabled(databaseName))
        {
            admin.enableTable(databaseName);
        }
    }

    private void addColumn(TableInfo tableInfo) throws IOException
    {
        HColumnDescriptor columnDescriptor;
        columnDescriptor = getColumnDescriptor(tableInfo);
        admin.addColumn(databaseName, columnDescriptor);
    }

    private void createTable(List<TableInfo> tableInfos) throws IOException
    {
        HTableDescriptor hTableDescriptor = getTableMetaData(tableInfos);
        admin.createTable(hTableDescriptor);
    }

    /**
     * drop schema method drop the table
     */
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            try
            {
                HTableDescriptor hTableDescriptor = null;
                if (admin.isTableAvailable(databaseName))
                {
                    if (admin.isTableEnabled(databaseName))
                    {
                        hTableDescriptor = admin.getTableDescriptor(databaseName.getBytes());
                        admin.disableTable(databaseName);
                    }
                    for (TableInfo tableInfo : tableInfos)
                    {
                        if (tableInfo != null && tableInfo.getTableName() != null
                                && hTableDescriptor.getFamily(tableInfo.getTableName().getBytes()) != null)
                        {
                            admin.deleteColumn(databaseName, tableInfo.getTableName());
                        }
                    }
                }
            }
            catch (TableNotFoundException tnfe)
            {
                logger.error("Table doesn't exist, Caused by ", tnfe);
                throw new SchemaGenerationException(tnfe, "Hbase");
            }
            catch (IOException ioe)
            {
                logger.error("Table isn't in enabled state, Caused by", ioe);
                throw new SchemaGenerationException(ioe, "Hbase");
            }
            finally
            {
                try
                {
                    admin.enableTable(databaseName);
                }
                catch (IOException ioe)
                {
                    logger.error("Table isn't in enabled state, Caused by", ioe);
                    throw new SchemaGenerationException(ioe, "Hbase");
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
        String message = null;
        for (String host : hosts)
        {
            vaildateHostPort(host, port);

            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hbase.master", host + ":" + port);
            conn = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                    .getConnection() : null;
            if (conn != null && conn.getProperties() != null)
            {
                String zookeeperHost = conn.getProperties().getProperty("hbase.zookeeper.quorum").trim();
                String zookeeperPort = conn.getProperties().getProperty("hbase.zookeeper.property.clientPort").trim();
                vaildateHostPort(zookeeperHost, zookeeperPort);
                hadoopConf.set("hbase.zookeeper.quorum", zookeeperHost != null ? zookeeperHost : host);
                hadoopConf.set("hbase.zookeeper.property.clientPort", zookeeperPort != null ? zookeeperPort
                        : DEFAULT_ZOOKEEPER_PORT);
            }
            else
            {
                hadoopConf.set("hbase.zookeeper.quorum", host);
                hadoopConf.set("hbase.zookeeper.property.clientPort", DEFAULT_ZOOKEEPER_PORT);
            }
            HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
            try
            {
                admin = new HBaseAdmin(conf);
                return true;
            }
            catch (MasterNotRunningException mnre)
            {
                message = mnre.getMessage();
                logger.error("Master not running exception, Caused by:", mnre);
            }
            catch (ZooKeeperConnectionException zkce)
            {
                message = zkce.getMessage();
                logger.error("Unable to connect to zookeeper, Caused by:", zkce);
            }
            catch (IOException ioe)
            {
                message = ioe.getMessage();
                logger.error("I/O exception, Caused by:", ioe);
            }
        }
        throw new SchemaGenerationException("Master not running exception, Caused by:" + message);
    }

    /**
     * Validate host and port.
     * 
     * @param host
     * @param port
     */
    private void vaildateHostPort(String host, String port)
    {
        if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
        {
            logger.error("Host or port should not be null / port should be numeric");
            throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
        }
    }

    /**
     * get Table metadata method returns the HTableDescriptor of table for given
     * tableInfo
     * 
     * @return HHTableDescriptor object for tableInfo
     */
    private HTableDescriptor getTableMetaData(List<TableInfo> tableInfos)
    {
        HTableDescriptor tableDescriptor = new HTableDescriptor(databaseName);
        Properties tableProperties = null;
        schemas = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getSchemas() : null;
        if (schemas != null && !schemas.isEmpty())
        {
            for (Schema s : schemas)
            {
                if (s.getName() != null && s.getName().equalsIgnoreCase(databaseName))
                {
                    tableProperties = s.getSchemaProperties();
                    tables = s.getTables();
                }
            }
        }
        for (TableInfo tableInfo : tableInfos)
        {
            if (tableInfo != null)
            {
                HColumnDescriptor hColumnDescriptor = getColumnDescriptor(tableInfo);
                tableDescriptor.addFamily(hColumnDescriptor);
            }
        }
        if (tableProperties != null)
        {
            for (Object o : tableProperties.keySet())
            {
                tableDescriptor.setValue(Bytes.toBytes(o.toString()), Bytes.toBytes(tableProperties.get(o).toString()));
            }
        }
        return tableDescriptor;
    }

    private HColumnDescriptor getColumnDescriptor(TableInfo tableInfo)
    {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(tableInfo.getTableName());
        setColumnFamilyProperties(hColumnDescriptor, tableInfo.getTableName());
        return hColumnDescriptor;
    }

    private void setColumnFamilyProperties(HColumnDescriptor columnDescriptor, String columnName)
    {
        schemas = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getSchemas() : null;
        if (schemas != null && !schemas.isEmpty())
        {
            for (Schema s : schemas)
            {
                if (s.getName() != null && s.getName().equalsIgnoreCase(databaseName))
                {
                    tables = s.getTables();
                }
            }
        }
        dataStore = HBasePropertyReader.hsmd.getDataStore();
        if (dataStore != null)
        {
            if (tables != null && !tables.isEmpty())
            {
                for (Table t : tables)
                {
                    Properties columnProperties = t.getProperties();
                    if (t.getName() != null && t.getName().equalsIgnoreCase(columnDescriptor.getNameAsString())
                            && columnProperties != null)
                    {
                        for (Object o : columnProperties.keySet())
                        {
                            columnDescriptor.setValue(Bytes.toBytes(o.toString()),
                                    Bytes.toBytes(columnProperties.get(o).toString()));
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return true;
    }

}
