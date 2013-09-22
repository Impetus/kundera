/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CassandraOperationUtils
{

    private Cassandra.Client cassandra_client;

    private static Logger logger = Logger.getLogger(CassandraOperationUtils.class);

    private static Runtime runTime;

    /**
     * Start mongo server.
     * 
     * @param args
     * 
     * @param runtime
     *            the runtime
     * @return the process
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void startCassandraServer(Boolean performDeleteData, Runtime runtime, String startCassandraServerCommand)
            throws IOException, InterruptedException
    {
        logger.info("Starting casssandra server ...........");
        this.runTime = runtime;
        while (checkOnProcess(runtime))
        {
            stopCassandraServer(performDeleteData, runtime);
            TimeUnit.SECONDS.sleep(2);
        }

        if (performDeleteData)
        {
            runtime.exec("rm -rf /var/lib/cassandra/*").waitFor();
            runtime.exec("mkdir /var/lib/cassandra/").waitFor();
            runtime.exec("chmod 777 -R /var/lib/cassandra/").waitFor();
        }
        runtime.exec(startCassandraServerCommand).waitFor();
        TimeUnit.SECONDS.sleep(5);
        while (!checkOnProcess(runtime))
        {
            TimeUnit.SECONDS.sleep(2);
        }
        logger.info("started..............");
    }

    /**
     * Stop mongo server.
     * 
     * @param runtime
     *            the runtime
     * @param br
     *            the br
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void stopCassandraServer(Boolean performDeleteData, Runtime runtime) throws IOException,
            InterruptedException
    {
        logger.info("Stoping casssandra server..");
        this.runTime = runtime;
        Process process = runtime.exec("jps");
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line = null;
        while ((line = br.readLine()) != null)
        {
            if (line.contains("CassandraDaemon"))
            {
                int idx;
                idx = line.indexOf("CassandraDaemon");

                runtime.exec("kill -9 " + line.substring(0, idx - 1));
                // logger.info("Killed process " + line.substring(0, idx
                // - 1));
                TimeUnit.SECONDS.sleep(5);
                if (performDeleteData)
                {
                    deleteCassandraFolders("/var/lib/cassandra/data/");
                    deleteCassandraFolders("/var/lib/cassandra/data/system/");
                    deleteCassandraFolders("/var/lib/cassandra/commitlog/");
                    deleteCassandraFolders("/var/lib/cassandra/saved_caches/");
                    deleteCassandraFolders("/var/log/cassandra/");
                }
            }
        }

        logger.info("stopped..............");
    }

    public void createKeysapce(String keyspace, boolean performCleanup, String host, String columnFamily, int port)
            throws InterruptedException, IOException
    {
        logger.info("creating keyspace " + keyspace + " and column family " + columnFamily);
        cassandra_client = null;
        if (cassandra_client == null)
        {
            initiateClient(host, this.runTime, port);
        }
        KsDef ksDef = null;

        try
        {
            ksDef = cassandra_client.describe_keyspace(keyspace);
            if (performCleanup)
            {
                dropKeyspace(keyspace, host, port);
                createKeyspaceAndColumnFamily(keyspace, columnFamily, ksDef);
            }
        }
        catch (NotFoundException e)
        {
            createKeyspaceAndColumnFamily(keyspace, columnFamily, ksDef);
        }

        catch (InvalidRequestException e)
        {
            logger.error(e);
        }
        catch (TException e)
        {
            logger.error(e);
        }

        logger.info("created keyspace " + keyspace + " and column family " + columnFamily);
        TimeUnit.SECONDS.sleep(3);

    }

    /**
     * @param keyspace
     * @param columnFamily
     */
    private void createKeyspaceAndColumnFamily(String keyspace, String columnFamily, KsDef ksDef)
    {

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        CfDef cfDef = new CfDef(keyspace, columnFamily); // thrift client.
        cfDef.setDefault_validation_class("UTF8Type");
        cfDef.setKey_validation_class("UTF8Type");
        cfDef.setComparator_type("UTF8Type");

        CfDef cfDef1 = new CfDef(keyspace, "kthrift" + columnFamily); // kundera thrift client.
        cfDef1.setDefault_validation_class("UTF8Type");
        cfDef1.setKey_validation_class("UTF8Type");
        cfDef1.setComparator_type("UTF8Type");


        CfDef cfDef2 = new CfDef(keyspace, "kpelops" + columnFamily); // kundera pelops client.
        cfDef2.setDefault_validation_class("UTF8Type");
        cfDef2.setKey_validation_class("UTF8Type");
        cfDef2.setComparator_type("UTF8Type");


        CfDef cfDef3 = new CfDef(keyspace, "pelops" + columnFamily); // pelops client.
        cfDef3.setDefault_validation_class("UTF8Type");
        cfDef3.setKey_validation_class("UTF8Type");
        cfDef3.setComparator_type("UTF8Type");

        cfDefs.add(cfDef);
        cfDefs.add(cfDef1); 
        cfDefs.add(cfDef2);
        cfDefs.add(cfDef3); 
        ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
        Map<String, String> strategy_options = new HashMap<String, String>();
        strategy_options.put("replication_factor", "1");
        ksDef.setStrategy_options(strategy_options);

        try
        {
            cassandra_client.system_add_keyspace(ksDef);
        }
        catch (InvalidRequestException e1)
        {
            logger.error(e1);
        }
        catch (SchemaDisagreementException e1)
        {
            logger.error(e1);
        }
        catch (TException e1)
        {
            logger.error(e1);
        }
    }

    public void dropKeyspace(String keyspace, String host, int port) throws InterruptedException, IOException
    {
        cassandra_client = null;
        if (cassandra_client == null)
        {
            initiateClient(host, this.runTime, port);
        }
        try
        {
            cassandra_client.system_drop_keyspace(keyspace);
        }
        catch (InvalidRequestException e)
        {
            // logger.error(e);
        }
        catch (SchemaDisagreementException e)
        {
            logger.error(e);
        }
        catch (TException e)
        {
            logger.error(e);
        }
        TimeUnit.SECONDS.sleep(3);

    }

    private void deleteCassandraFolders(String dir)
    {
        // logger.info("Cleaning up folder " + dir);
        File directory = new File(dir);
        // Get all files in directory
        File[] files = directory.listFiles();
        for (File file : files)
        {
            // Delete each file
            if (!file.delete())
            {
                // Failed to delete file
                // logger.info("Failed to delete " + file);
            }
        }
    }

    /**
     * @throws IOException
     */
    private boolean checkOnProcess(Runtime runtime) throws IOException
    {
        Process process = runtime.exec("jps");
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line = null;
        boolean found = false;
        while ((line = br.readLine()) != null)
        {
            if (line.contains("CassandraDaemon"))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private void initiateClient(String host, Runtime runtime, int port) throws InterruptedException, IOException
    {
        while (checkOnProcess(runtime))
        {
            TSocket socket = new TSocket(host, port);
            TTransport transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            cassandra_client = new Cassandra.Client(protocol);
            try
            {
                if (!socket.isOpen())
                {
                    socket.open();
                }
            }
            catch (TTransportException e)
            {
                logger.error(e);
            }
            catch (NumberFormatException e)
            {
                logger.error(e);
            }
            TimeUnit.SECONDS.sleep(3);
            return;
        }
    }

}
