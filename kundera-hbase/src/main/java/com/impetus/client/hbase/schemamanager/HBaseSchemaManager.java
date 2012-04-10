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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.ClientType;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;

/**
 * Manages auto schema operation {@code ScheamOperationType} for Hbase data
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

    @Override
    /**
     * Export schema handles the handleOperation method.
     */
    public void exportSchema()
    {
        super.exportSchema(ClientType.HBASE);
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
            HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
            try
            {
                HTableDescriptor descriptor = admin.getTableDescriptor(tableInfo.getTableName().getBytes());
                if (!hTableDescriptor.equals(descriptor))
                {
                    admin.disableTable(tableInfo.getTableName().getBytes());
                    HColumnDescriptor[] descriptors = descriptor.getColumnFamilies();
                    for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
                    {
                        boolean found = false;
                        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnInfo.getColumnName());
                        for (HColumnDescriptor hColumnDescriptor : descriptors)
                        {
                            if (hColumnDescriptor.equals(columnDescriptor))
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
                    for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
                    {
                        boolean found = false;
                        HColumnDescriptor columnDescriptor = new HColumnDescriptor(
                                embeddedColumnInfo.getEmbeddedColumnName());
                        for (HColumnDescriptor hColumnDescriptor : descriptors)
                        {
                            if (hColumnDescriptor.equals(columnDescriptor))
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
            catch (IOException e)
            {
                try
                {
                    admin.createTable(hTableDescriptor);
                }
                catch (IOException e1)
                {
                    logger.error("check for network connection caused by" + e.getMessage());
                    throw new SchemaGenerationException(e, "Hbase");
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
            HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
            try
            {
                if (!hTableDescriptor.equals(admin.getTableDescriptor(tableInfo.getTableName().getBytes())))
                {
                    throw new SchemaGenerationException("Hbase", tableInfo.getTableName());
                }
            }
            catch (IOException e)
            {
                logger.error("either check for network connection or table isn't in enabled state caused by"
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
                logger.error("either table isn't in enabled state or some network problem caused by "
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
                logger.error("table isn't in enabled state caused by" + ioex1.getMessage());
                throw new SchemaGenerationException(ioex1, "Hbase");
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
                try
                {
                    admin.disableTable(tableInfo.getTableName());
                    admin.deleteTable(tableInfo.getTableName());
                }
                catch (TableNotFoundException e)
                {
                    logger.error("table doesn't exist caused by " + e.getMessage());
                    throw new SchemaGenerationException(e, "Hbase");
                }
                catch (IOException e)
                {
                    logger.error("table isn't in enabled state caused by" + e.getMessage());
                    throw new SchemaGenerationException(e, "Hbase");
                }
            }
        }
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
        if (kundera_client.equalsIgnoreCase("Hbase"))
        {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hbase.master", host + ":" + port);
            HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
            try
            {
                admin = new HBaseAdmin(conf);
            }
            catch (MasterNotRunningException e)
            {
                logger.error("master not running exception caused by" + e.getMessage());
                throw new SchemaGenerationException(e, "Hbase");
            }
            catch (ZooKeeperConnectionException e)
            {
                logger.equals("unable to connect to zookeeper caused by" + e.getMessage());
                throw new SchemaGenerationException(e, "Hbase");
            }
            return true;
        }
        return false;
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
        if (tableInfo.getColumnMetadatas() != null)
        {
            for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas())
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnInfo.getColumnName());
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
        }
        if (tableInfo.getEmbeddedColumnMetadatas() != null)
        {
            for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo.getEmbeddedColumnMetadatas())
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(embeddedColumnInfo.getEmbeddedColumnName());
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
        }
        return hTableDescriptor;
    }

}
