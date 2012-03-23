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
package com.impetus.client.hbase.junits;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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
    private static final Logger logger = LoggerFactory.getLogger(HBaseCli.class);
    public static void main(String arg[])
    {
        HBaseCli cli = new HBaseCli();
        cli.init();
    }
    
    public void init()
    {
        File workingDirectory = new File("./");
        Configuration conf = new Configuration();
        System.setProperty( "test.build.data", workingDirectory.getAbsolutePath() );
        conf.set( "test.build.data", new File( workingDirectory, "zookeeper" ).getAbsolutePath() );
        conf.set( "fs.default.name", "file:///" );
        try
        {
            conf.set( HConstants.HBASE_DIR, new File( workingDirectory, "hbase" ).toURI().toURL().toString() );
        }
        catch (MalformedURLException e1)
        {
            logger.error(e1.getMessage());
        }

        Configuration hbaseConf = HBaseConfiguration.create(conf);
        HBaseTestingUtility utility = new HBaseTestingUtility(hbaseConf);
        try
        {
            utility.startMiniCluster();
            HTable table = utility.createTable("test".getBytes(), "testcol".getBytes());
            utility.getHBaseAdmin().disableTable("test");
            utility.getHBaseAdmin().addColumn("test", new HColumnDescriptor("testColFamily"));
            utility.getHBaseAdmin().enableTable("test");
            logger.info("Server is running : " +utility.getHBaseAdmin().isMasterRunning());
            
            Put p = new Put(Bytes.toBytes("1"));
            p.add(Bytes.toBytes("testColFamily"), Bytes.toBytes("col1"),"col1".getBytes());
            table.put(p);
            logger.info("Table exist:" + utility.getHBaseAdmin().tableExists("test"));
            Get g = new Get(Bytes.toBytes("1"));
            Result r = table.get(g);
            logger.info("Row count:" + r.list().size());
            utility.getHBaseAdmin().disableTable("test");
            logger.info("Deleting table...");
            utility.getHBaseAdmin().deleteTable("test");
            logger.info("Shutting down now...");
            utility.shutdownMiniCluster();
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
        }
    }

}
