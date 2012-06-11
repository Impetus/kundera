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
package com.impetus.kundera.tests.cli;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HBase client.
 * 
 * @author vivek.mishra
 * 
 */
public final class HBaseCli
{

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(HBaseCli.class);

    /** The utility. */
    private static HBaseTestingUtility utility;

    private static Boolean isStarted = false;

    private static File zkDir;

    private static File masterDir;

    /**
     * The main method.
     * 
     * @param arg
     *            the arguments
     */
    public static void main(String arg[])
    {
        startCluster();
    }

    /**
     * Starts a new cluster.
     */
    public static void startCluster()
    {
        if (!isStarted)
        {
            File workingDirectory = new File("./");
            Configuration conf = new Configuration();
            System.setProperty("test.build.data", workingDirectory.getAbsolutePath());
            conf.set("test.build.data", new File(workingDirectory, "zookeeper").getAbsolutePath());
            conf.set("fs.default.name", "file:///");
            conf.set("zookeeper.session.timeout", "180000");
            conf.set("hbase.zookeeper.peerport", "2888");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            try
            {
                masterDir = new File(workingDirectory, "hbase");
                conf.set(HConstants.HBASE_DIR, masterDir.toURI().toURL().toString());
            }
            catch (MalformedURLException e1)
            {
                logger.error(e1.getMessage());
            }

            Configuration hbaseConf = HBaseConfiguration.create(conf);
            utility = new HBaseTestingUtility(hbaseConf);
            try
            {
                MiniZooKeeperCluster zkCluster = new MiniZooKeeperCluster(conf);
                zkCluster.setDefaultClientPort(2181);
                zkCluster.setTickTime(18000);
                zkDir = new File(utility.getClusterTestDir().toString());
                zkCluster.startup(zkDir);
                utility.setZkCluster(zkCluster);
                utility.startMiniCluster();
                utility.getHBaseCluster().startMaster();
            }
            catch (Exception e)
            {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
            isStarted = true;
        }
    }

    /**
     * Creates the table.
     * 
     * @param tableName
     *            the table name
     */
    public static void createTable(String tableName)
    {
        try
        {
            if (!utility.getHBaseAdmin().tableExists(tableName))
            {
                utility.createTable(tableName.getBytes(), tableName.getBytes());
            }
            else
            {
                logger.info("Table:" + tableName + " already exist:");
            }
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }
    }

    /**
     * Adds the column family.
     * 
     * @param tableName
     *            the table name
     * @param columnFamily
     *            the column family
     */
    public static void addColumnFamily(String tableName, String columnFamily)
    {
        try
        {
            utility.getHBaseAdmin().disableTable(tableName);
            utility.getHBaseAdmin().addColumn(tableName, new HColumnDescriptor(columnFamily));
            utility.getHBaseAdmin().enableTable(tableName);
        }
        catch (InvalidFamilyOperationException ife)
        {
            logger.info("Column family:" + columnFamily + " already exist!");
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }
    }

    /**
     * Destroyes cluster.
     */
    public static void stopCluster()
    {
        try
        {
            if (utility != null)
            {
                utility.cleanupTestDir();
                utility.shutdownMiniCluster();
                utility = null;
                FileUtil.fullyDelete(zkDir);
                FileUtil.fullyDelete(masterDir);
            }
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }
        /*
         * DO NOT DELETE IT! HTable table =
         * utility.createTable("test".getBytes(), "testcol".getBytes());
         * utility.getHBaseAdmin().disableTable("test");
         * utility.getHBaseAdmin().addColumn("test", new
         * HColumnDescriptor("testColFamily"));
         * utility.getHBaseAdmin().enableTable("test");
         * logger.info("Server is running : "
         * +utility.getHBaseAdmin().isMasterRunning());
         * 
         * Put p = new Put(Bytes.toBytes("1"));
         * p.add(Bytes.toBytes("testColFamily"),
         * Bytes.toBytes("col1"),"col1".getBytes()); table.put(p);
         * logger.info("Table exist:" +
         * utility.getHBaseAdmin().tableExists("test")); Get g = new
         * Get(Bytes.toBytes("1")); Result r = table.get(g);
         * logger.info("Row count:" + r.list().size());
         * utility.getHBaseAdmin().disableTable("test");
         * logger.info("Deleting table...");
         * utility.getHBaseAdmin().deleteTable("test");
         * logger.info("Shutting down now..."); utility.shutdownMiniCluster();
         */
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static boolean isStarted()
    {
        return isStarted;
    }
}
