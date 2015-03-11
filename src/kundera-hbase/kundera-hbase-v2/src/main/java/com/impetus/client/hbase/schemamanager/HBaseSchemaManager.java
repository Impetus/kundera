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
package com.impetus.client.hbase.schemamanager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.config.HBasePropertyReader;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.Table;
import com.impetus.kundera.configure.schema.CollectionColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author Pragalbh Garg
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

    private Map<String, Properties> externalProperties;

    /**
     * Instantiates a new h base schema manager.
     * 
     * @param clientFactory
     *            the client factory
     * @param puProperties
     *            the pu properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public HBaseSchemaManager(String clientFactory, Map<String, Object> puProperties,
            final KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, puProperties, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#exportSchema
     * (java.lang.String, java.util.List)
     */
    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#update
     * (java.util.List)
     */
    @Override
    protected void update(List<TableInfo> tableInfos)
    {
        createOrUpdateSchema(tableInfos, true);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#validate
     * (java.util.List)
     */
    @Override
    protected void validate(List<TableInfo> tableInfos)
    {
        try
        {
            if (isNamespaceAvailable(databaseName))
            {
                for (TableInfo tableInfo : tableInfos)
                {

                    if (tableInfo != null)
                    {
                        HTableDescriptor hTableDescriptor = admin.getTableDescriptor((databaseName + ":" + tableInfo
                                .getTableName()).getBytes());
                        boolean columnFamilyFound = false;
                        Boolean f = false;
                        for (HColumnDescriptor columnDescriptor : hTableDescriptor.getColumnFamilies())
                        {

                            if (!columnFamilyFound
                                    && columnDescriptor.getNameAsString().equalsIgnoreCase(tableInfo.getTableName()))
                            {
                                columnFamilyFound = true;
                            }

                            for (CollectionColumnInfo cci : tableInfo.getCollectionColumnMetadatas())
                            {
                                if (columnDescriptor.getNameAsString().equalsIgnoreCase(cci.getCollectionColumnName()))
                                {
                                    f = true;
                                    break;
                                }

                            }
                            if (!(columnFamilyFound || f))
                            {
                                throw new SchemaGenerationException("column " + tableInfo.getTableName()
                                        + " does not exist in table " + databaseName + "", "Hbase", databaseName,
                                        tableInfo.getTableName());
                            }
                            // TODO make a check for sec table
                        }

                    }
                }
            }
            else
            {
                throw new SchemaGenerationException("Namespace" + databaseName + "does not exist", "HBase",
                        databaseName);
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create_drop
     * (java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos)
    {
        createOrUpdateSchema(tableInfos, false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create
     * (java.util.List)
     */
    @Override
    protected void create(List<TableInfo> tableInfos)
    {
        createOrUpdateSchema(tableInfos, false);
    }

    /**
     * Drop.
     */
    protected void drop()
    {
        try
        {
            admin.disableTables(databaseName + ":.*");
            admin.deleteTables(databaseName + ":.*");
        }
        catch (TableNotFoundException tnfe)
        {
            logger.error("Table doesn't exist, Caused by ", tnfe);
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
                admin.deleteNamespace(databaseName);
            }
            catch (IOException ioe)
            {
                logger.error("Table isn't in enabled state, Caused by", ioe);
                throw new SchemaGenerationException(ioe, "Hbase");
            }
        }
    }

    /**
     * Creates the or update schema.
     * 
     * @param tableInfos
     *            the table infos
     * @param isUpdate
     *            the is update
     */
    private void createOrUpdateSchema(List<TableInfo> tableInfos, Boolean isUpdate)

    {
        createNamespace(isUpdate);
        readExternalProperties();
        Map<Class<?>, EntityType<?>> entityMap = kunderaMetadata.getApplicationMetadata()
                .getMetaModelBuilder(puMetadata.getPersistenceUnitName()).getManagedTypes();
        // iterating all classes of pu to generate schema
        for (Class<?> clazz : entityMap.keySet())
        {
            String tablename = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz).getTableName();
            HTableDescriptor hTableDescriptor = getTableDescriptor(clazz, entityMap.get(clazz), tablename);
            createOrUpdateTable(tablename, hTableDescriptor);
        }
    }

    /**
     * Gets the table descriptor.
     * 
     * @param clazz
     *            the clazz
     * @param entityType
     *            the entity type
     * @param tableName
     *            the table name
     * @return the table descriptor
     */
    private HTableDescriptor getTableDescriptor(Class<?> clazz, EntityType<?> entityType, String tableName)
    {
        try
        {
            AbstractManagedType<?> ent = (AbstractManagedType<?>) entityType;
            HTableDescriptor tableDescriptor = null;
            String hTableName = HBaseUtils.getHTableName(databaseName, tableName);
            if (!admin.isTableAvailable(TableName.valueOf(hTableName)))
            {
                tableDescriptor = new HTableDescriptor(TableName.valueOf(hTableName));
            }
            else
            {
                tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hTableName));
            }
            addColumnFamilyAndSetProperties(tableDescriptor, tableName);

            // Add column families for @SecondaryTable
            List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) ent.getEntityAnnotation())
                    .getSecondaryTablesName();
            for (String secTable : secondaryTables)
            {
                addColumnFamilyAndSetProperties(tableDescriptor, secTable);
            }

            // @CollectionTable is not handled.
            return tableDescriptor;
        }
        catch (TableNotFoundException tnfex)
        {
            logger.error("Either table isn't in enabled state or some network problem, Caused by: ", tnfex);
            throw new SchemaGenerationException(tnfex, "Hbase");
        }
        catch (IOException ioex)
        {
            logger.error("Either table isn't in enabled state or some network problem, Caused by: ", ioex);
            throw new SchemaGenerationException(ioex, "Hbase");
        }
    }

    /**
     * Adds the column family and set properties.
     * 
     * @param tableDescriptor
     *            the table descriptor
     * @param secTable
     *            the sec table
     */
    private void addColumnFamilyAndSetProperties(HTableDescriptor tableDescriptor, String secTable)
    {
        if (!tableDescriptor.hasFamily(secTable.getBytes()))
        {
            HColumnDescriptor hColumnDescriptor = getColumnDescriptor(secTable);
            tableDescriptor.addFamily(hColumnDescriptor);
            setExternalProperties(tableDescriptor.getNameAsString(), hColumnDescriptor);
        }
    }

    /**
     * Sets the external properties.
     * 
     * @param name
     *            the name
     * @param hColumnDescriptor
     *            the h column descriptor
     */
    private void setExternalProperties(String name, HColumnDescriptor hColumnDescriptor)
    {
        Properties props = externalProperties != null ? externalProperties.get(name) : null;
        if (props != null && !props.isEmpty())
        {
            for (Object o : props.keySet())
            {
                hColumnDescriptor.setValue(Bytes.toBytes(o.toString()), Bytes.toBytes(props.get(o).toString()));
            }
        }
    }

    /**
     * Creates the namespace.
     * 
     * @param isUpdate
     *            the is update
     */
    private void createNamespace(boolean isUpdate)
    {
        boolean isNameSpaceAvailable = isNamespaceAvailable(databaseName);
        if (isNameSpaceAvailable && !isUpdate)
        {
            drop();
        }

        if (!(isNameSpaceAvailable && isUpdate))
        {
            try
            {
                NamespaceDescriptor descriptor = NamespaceDescriptor.create(databaseName).build();
                admin.createNamespace(descriptor);
            }
            catch (IOException ioex)
            {
                logger.error("Either table isn't in enabled state or some network problem, Caused by: ", ioex);
                throw new SchemaGenerationException(ioex, "Hbase");
            }
        }

    }

    /**
     * Checks if is namespace available.
     * 
     * @param databaseName
     *            the database name
     * @return true, if is namespace available
     */
    private boolean isNamespaceAvailable(String databaseName)
    {
        try
        {
            for (NamespaceDescriptor ns : admin.listNamespaceDescriptors())
            {
                if (ns.getName().equals(databaseName))
                {
                    return true;
                }
            }
            return false;
        }
        catch (IOException ioex)
        {
            logger.error("Either table isn't in enabled state or some network problem, Caused by: ", ioex);
            throw new SchemaGenerationException(ioex, "Either table isn't in enabled state or some network problem.");
        }
    }

    /**
     * Creates the or update table.
     * 
     * @param tablename
     *            the tablename
     * @param hTableDescriptor
     *            the h table descriptor
     */
    private void createOrUpdateTable(String tablename, HTableDescriptor hTableDescriptor)
    {
        try
        {
            if (admin.isTableAvailable(databaseName + ":" + tablename))
            {
                admin.modifyTable(databaseName + ":" + tablename, hTableDescriptor);
            }
            else
            {
                admin.createTable(hTableDescriptor);
            }
        }
        catch (IOException ioex)
        {
            logger.error("Either table isn't in enabled state or some network problem, Caused by: ", ioex);
            throw new SchemaGenerationException(ioex, "Either table isn't in enabled state or some network problem.");
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema()
    {
        if (operation != null && operation.equalsIgnoreCase("create-drop"))
        {
            drop();
        }
        operation = null;
        admin = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#initiateClient
     * ()
     */
    @Override
    protected boolean initiateClient()
    {
        try
        {
            Connection connection = ConnectionFactory.createConnection();
            admin = (HBaseAdmin) connection.getAdmin();
            return true;
        }
        catch (IOException e)
        {
            throw new SchemaGenerationException("Master not running exception, Caused by:" + e.getMessage());
        }

        /*
         * String message = null; for (String host : hosts) {
         * vaildateHostPort(host, port);
         * 
         * Configuration hadoopConf = new Configuration();
         * hadoopConf.set("hbase.master", host + ":" + port); conn =
         * HBasePropertyReader.hsmd.getDataStore() != null ?
         * HBasePropertyReader.hsmd.getDataStore() .getConnection() : null; if
         * (conn != null && conn.getProperties() != null) { String zookeeperHost
         * = conn.getProperties().getProperty("hbase.zookeeper.quorum").trim();
         * String zookeeperPort =
         * conn.getProperties().getProperty("hbase.zookeeper.property.clientPort"
         * ).trim(); vaildateHostPort(zookeeperHost, zookeeperPort);
         * hadoopConf.set("hbase.zookeeper.quorum", zookeeperHost != null ?
         * zookeeperHost : host);
         * hadoopConf.set("hbase.zookeeper.property.clientPort", zookeeperPort
         * != null ? zookeeperPort : DEFAULT_ZOOKEEPER_PORT); } else {
         * hadoopConf.set("hbase.zookeeper.quorum", host);
         * hadoopConf.set("hbase.zookeeper.property.clientPort",
         * DEFAULT_ZOOKEEPER_PORT); } Configuration conf =
         * HBaseConfiguration.create(hadoopConf); try { admin = new
         * HBaseAdmin(conf); return true; } catch (MasterNotRunningException
         * mnre) { message = mnre.getMessage();
         * logger.error("Master not running exception, Caused by:", mnre); }
         * catch (ZooKeeperConnectionException zkce) { message =
         * zkce.getMessage();
         * logger.error("Unable to connect to zookeeper, Caused by:", zkce); }
         * catch (IOException ioe) { message = ioe.getMessage();
         * logger.error("I/O exception, Caused by:", ioe); } } throw new
         * SchemaGenerationException("Master not running exception, Caused by:"
         * + message);
         */
    }

    // /**
    // * Vaildate host port.
    // *
    // * @param host the host
    // * @param port the port
    // */
    // private void vaildateHostPort(String host, String port)
    // {
    // if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
    // {
    // logger.error("Host or port should not be null / port should be numeric");
    // throw new
    // IllegalArgumentException("Host or port should not be null / port should be numeric");
    // }
    // }

    /**
     * Gets the column descriptor.
     * 
     * @param tableName
     *            the table name
     * @return the column descriptor
     */
    private HColumnDescriptor getColumnDescriptor(String tableName)
    {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(tableName);
        return hColumnDescriptor;
    }

    /**
     * Read external properties.
     */
    private void readExternalProperties()
    {
        schemas = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getSchemas() : null;
        List<Table> tables = null;
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
        if (tables != null && !tables.isEmpty())
        {
            for (Table t : tables)
            {
                externalProperties.put(HBaseUtils.getHTableName(databaseName, t.getName()), t.getProperties());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.SchemaManager#validateEntity
     * (java.lang.Class)
     */
    @Override
    public boolean validateEntity(Class clazz)
    {
        // TODO Auto-generated method stub
        return true;
    }

}
