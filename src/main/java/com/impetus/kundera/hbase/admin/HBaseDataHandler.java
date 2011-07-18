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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.hbase.client.HBaseData;
import com.impetus.kundera.hbase.client.Reader;
import com.impetus.kundera.hbase.client.Writer;
import com.impetus.kundera.hbase.client.service.HBaseReader;
import com.impetus.kundera.hbase.client.service.HBaseWriter;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author impetus
 */
public class HBaseDataHandler implements DataHandler
{

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

    @Override
    public HBaseData readData(final String tableName, final String columnFamily, final String[] columnName,
            final String rowKey) throws IOException
    {
        return hbaseReader.LoadData(gethTable(tableName), columnFamily, columnName, rowKey);
    }

    @Override
    public void writeData(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)
            throws IOException
    {

        hbaseWriter.writeColumns(gethTable(tableName), columnFamily, rowKey, columns, e);
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
