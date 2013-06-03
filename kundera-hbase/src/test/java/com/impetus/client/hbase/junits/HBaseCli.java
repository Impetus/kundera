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
package com.impetus.client.hbase.junits;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HBase client.
 * 
 * @author vivek.mishra
 * 
 */
public class HBaseCli
{
    /** The utility. */
    public static HBaseTestingUtility utility;

    private static final Logger logger = LoggerFactory.getLogger(HBaseCli.class);

    private static File zkDir;

    private static File masterDir;

    private MiniZooKeeperCluster zkCluster;

    private HTablePool hTablePool;

    public static void main(String arg[])
    {
        HBaseCli cli = new HBaseCli();
         cli.startCluster();;
    }

    public void startCluster()
    {
        File workingDirectory = new File("./");
        Configuration conf = new Configuration();
        System.setProperty("test.build.data", workingDirectory.getAbsolutePath());
        conf.set("test.build.data", new File(workingDirectory, "zookeeper").getAbsolutePath());
        conf.set("fs.default.name", "file:///");
        conf.set("zookeeper.session.timeout", "180000");
        conf.set("hbase.zookeeper.peerport", "2888");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("dfs.datanode.data.dir.perm", "700");
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
        
        // Change permission for dfs.data.dir, please refer
        // https://issues.apache.org/jira/browse/HBASE-5711 for more details.
        try {
            Process process = Runtime.getRuntime().exec("/bin/sh -c umask");
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            int rc = process.waitFor();
            if(rc == 0) {
                String umask = br.readLine();

                int umaskBits = Integer.parseInt(umask, 8);
                int permBits = 0777 & ~umaskBits;
                String perms = Integer.toString(permBits, 8);
                
                logger.info("Setting dfs.datanode.data.dir.perm to " + perms);
                utility.getConfiguration().set("dfs.datanode.data.dir.perm", perms);
            } else {
                logger.warn("Failed running umask command in a shell, nonzero return value");
            }
        } catch (Exception e) {
            // ignore errors, we might not be running on POSIX, or "sh" might not be on the path
            logger.warn("Couldn't get umask", e);
        }
        if (!checkIfServerRunning())
        {
            hTablePool = new HTablePool(conf, 1);
            try
            {
                zkCluster = new MiniZooKeeperCluster(conf);
                zkCluster.setDefaultClientPort(2181);
                zkCluster.setTickTime(18000);
                // utility.setupClusterTestDir();
                zkDir = new File(utility.getClusterTestDir().toString());

                zkCluster.startup(zkDir);
                utility.setZkCluster(zkCluster);
                utility.startMiniCluster();
                utility.getHBaseCluster().startMaster();
            }
            catch (Exception e)
            {
                e.printStackTrace();
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates the table.
     * 
     * @param tableName
     *            the table name
     */
    public void createTable(String tableName)
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

    public void createTable(byte[] tableName, byte[][] families)
    {
        try
        {
            utility.createTable(tableName, families);
        }
        catch (IOException e)
        {

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
    public void addColumnFamily(String tableName, String columnFamily)
    {
        try
        {
            utility.getHBaseAdmin().disableTable(tableName);
            utility.getHBaseAdmin().addColumn(tableName, new HColumnDescriptor(columnFamily));
            utility.getHBaseAdmin().enableTable(tableName);
            while (utility.getHBaseAdmin().isTableEnabled(columnFamily))
            {
                return;
            }

        }
        catch (InvalidFamilyOperationException ife)
        {
            logger.info("Column family:" + columnFamily + " already exist!", ife);
        }
        catch (IOException e)
        {
            logger.error("", e);
        }
    }

    public void dropTable(String tableName)
    {
        try
        {
            utility.getHBaseAdmin().disableTable(tableName);
            utility.getHBaseAdmin().deleteTable(tableName);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }
    }

    /**
     * Destroys cluster.
     */
    public static void stopCluster(String... tableName)
    {
        // try
        // {
        // if (utility != null)
        // {
        // // utility.getMiniHBaseCluster().shutdown();
        // // File workingDirectory = new File("./");
        // // utility.closeRegion("localhost");
        // utility.cleanupTestDir();
        // // utility.cleanupTestDir(dir.getAbsolutePath());
        // // ZooKeeperServer server = new ZooKeeperServer(zkDir, zkDir,
        // // 2000);
        // // ZooKeeperServerBean bean = new ZooKeeperServerBean(server);
        // // String path = (String)this.makeFullPath(null,bean);
        //
        // // MBeanS
        // // MBeanRegistry.getInstance().unregister(bean);
        // // MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        // // mbs.unregisterMBean(makeObjectName(path,bean));
        // // utility.getHbaseCluster().shutdown();
        // utility.shutdownMiniCluster();
        // FileUtil.fullyDelete(zkDir);
        // FileUtil.fullyDelete(masterDir);
        // utility = null;
        // isStarted = false;
        // }
        // }
        // catch (IOException e)
        // {
        // logger.error(e.getMessage());
        // }
        // catch (NullPointerException e)
        // {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // catch (Exception e)
        // {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        //
    }

    public static void cleanUp()
    {
        try
        {
            if (utility != null)
            {
                // utility.getMiniHBaseCluster().shutdown();
                // File workingDirectory = new File("./");
                // utility.closeRegion("localhost");
                utility.cleanupTestDir();
                // utility.cleanupTestDir(dir.getAbsolutePath());
                // ZooKeeperServer server = new ZooKeeperServer(zkDir, zkDir,
                // 2000);
                // ZooKeeperServerBean bean = new ZooKeeperServerBean(server);
                // String path = (String)this.makeFullPath(null,bean);

                // MBeanS
                // MBeanRegistry.getInstance().unregister(bean);
                // MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                // mbs.unregisterMBean(makeObjectName(path,bean));
                // utility.getHbaseCluster().shutdown();
                utility.shutdownMiniCluster();
                FileUtil.fullyDelete(zkDir);
                FileUtil.fullyDelete(masterDir);
                utility = null;
            }
        }
        catch (IOException e)
        {
            logger.error(e.getMessage());
        }
        catch (NullPointerException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 2181);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}
