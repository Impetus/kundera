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

package com.impetus.client.cassandra.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import lucandra.CassandraUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * The Class SolandraUtils.
 */
public class SolandraUtils
{
    /** The logger. */
    private static Logger logger = Logger.getLogger(SolandraUtils.class);

    /**
     * Start solandra server.
     */
    public void initializeSolandra(String contactNode, int port)
    {
        logger.info("Initializing Solandra...");
        new CassandraUtils();
        CassandraUtils.cacheInvalidationInterval = 0; // real-time

        try
        {
            // Create solandra specific schema
            createSolandraSpecificSchema(contactNode, port);
            
            // Start Solandra
            CassandraUtils.startupServer();
            
        }
        catch (Throwable t)
        {
            logger.error("errror while starting solandra schema:", t);
        }
        logger.info("Initialized Solandra...");

    }

    /**
     * Creates the cass schema.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void createSolandraSpecificSchema(String contactNode, int port) throws IOException
    {
        logger.info("Creating solandra specific schema (if not already exists)...");
        final String keySpace = "L"; // Solandra specific schema

        /* Solandra column families */
        final String termVecColumnFamily = "TI";
        final String docColumnFamily = "Docs";
        final String metaInfoColumnFamily = "TL";
        final String fieldCacheColumnFamily = "FC";
        final String schemaInfoColumnFamily = "SI";

        if (DatabaseDescriptor.getNonSystemTables().contains(keySpace))
        {
            logger.info("Solandra specific schema, \"L\" already exist, Noting to do.");
            return;
        }

        try
        {
            Thread.sleep(1000);

            int sleep = new Random().nextInt(6000);

            logger.info("\nSleeping " + sleep + "ms to stagger solandra schema creation\n");

            Thread.sleep(sleep);
        }
        catch (InterruptedException e1)
        {
            e1.printStackTrace();
            System.exit(2);
        }

        if (DatabaseDescriptor.getNonSystemTables().contains(keySpace))
        {
            logger.info("Solandra specific schema, \"L\" already exist, Noting to do.");
            return;
        }

        List<CfDef> cfs = new ArrayList<CfDef>();

        CfDef cf = new CfDef();
        cf.setName(docColumnFamily);
        cf.setComparator_type("BytesType");
        cf.setKey_cache_size(0);
        cf.setRow_cache_size(0);
        cf.setComment("Stores the document and field data for each doc with docId as key");
        cf.setKeyspace(keySpace);

        cfs.add(cf);

        cf = new CfDef();
        cf.setName(termVecColumnFamily);
        cf.setComparator_type("lucandra.VIntType");
        cf.setKey_cache_size(0);
        cf.setRow_cache_size(0);
        cf.setComment("Stores term information with indexName/field/term as composite key");
        cf.setKeyspace(keySpace);

        cfs.add(cf);

        cf = new CfDef();
        cf.setName(fieldCacheColumnFamily);
        cf.setComparator_type("lucandra.VIntType");
        cf.setKey_cache_size(0);
        cf.setRow_cache_size(0);
        cf.setComment("Stores term per doc per field");
        cf.setKeyspace(keySpace);

        cfs.add(cf);

        cf = new CfDef();
        cf.setName(metaInfoColumnFamily);
        cf.setComparator_type("BytesType");
        cf.setKey_cache_size(0);
        cf.setRow_cache_size(0);
        cf.setComment("Stores ordered list of terms for a given field with indexName/field as composite key");
        cf.setKeyspace(keySpace);

        cfs.add(cf);

        cf = new CfDef();
        cf.setName(schemaInfoColumnFamily);
        cf.setColumn_type("Super");
        cf.setComparator_type("BytesType");
        cf.setKey_cache_size(0);
        cf.setRow_cache_size(0);
        cf.setComment("Stores solr and index id information");
        cf.setKeyspace(keySpace);

        cfs.add(cf);

        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;
        KsDef solandraKS = new KsDef(keySpace, simple.getCanonicalName(), cfs);
        solandraKS.setReplication_factor(1);
        Cassandra.Client client = getClient(contactNode, port);

        try
        {
            client.send_system_add_keyspace(solandraKS);
        }
        catch (TException e)
        {
            throw new IOException(e);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }

        logger.info("Added Solandra specific schema. Sleeping for 10 seconds...");
        try
        {
            Thread.sleep(10000);
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    /**
     * Inits the client.
     * 
     * @return the client
     */
    private Cassandra.Client getClient(String contactNode, int port)
    {
        TSocket socket = new TSocket(contactNode, port);
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);

        try
        {
            if (!socket.isOpen())
            {
                socket.open();

            }
        }
        catch (TTransportException ttex)
        {
            logger.error(ttex.getMessage());
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage());
        }
        return client;

    }
}