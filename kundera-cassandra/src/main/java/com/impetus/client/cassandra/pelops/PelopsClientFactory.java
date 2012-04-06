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
package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

/**
 * A factory for creating PelopsCliobjects.
 */
public class PelopsClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(PelopsClientFactory.class);

    /** The index manager. */
    private IndexManager indexManager;

    /** The reader. */
    private EntityReader reader;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
     */
    @Override
    public void initialize()
    {
        // TODO StandardAnalyzer is thread safe. So it looks like indexManager
        // is threadsafe an hence using a single instance
        logger.info("Initializing Threadsafe Indexmanager. Is it really threadsafe?");

        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));

        reader = new CassandraEntityReader();

        schemaManager = new CassandraSchemaManager();
        schemaManager.exportSchema();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
     */
    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit());

        if (Pelops.getDbConnPool(poolName) == null)
        {
            Cluster cluster = new Cluster(contactNodes,
                    new IConnection.Config(Integer.parseInt(defaultPort), true, -1), false);

            Policy policy = PelopsUtils.getPoolConfigPolicy(persistenceUnitMetadata);

            // Add pool with specified policy. null means default operand
            // policy.
            Pelops.addPool(poolName, cluster, keyspace, policy, null);

        }
        // TODO return a thrift pool
        return null;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new PelopsClient(indexManager, reader, persistenceUnit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isClientThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
    @Override
    public void destroy()
    {
        indexManager.close();
        schemaManager.dropSchema();
        // Pelops.shutdown();
    }

}
