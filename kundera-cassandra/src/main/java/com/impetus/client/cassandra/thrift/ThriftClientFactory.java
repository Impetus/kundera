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
package com.impetus.client.cassandra.thrift;

import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.SimpleConnectionAuthenticator;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

/**
 * A factory of {@link ThriftClient}
 * Currently it uses Pelops for Connection pooling. Need to replace it with our own pooling.
 * Most of the code is borrowed from {@link PelopsClientFactory} 
 * @author amresh.singh
 */
public class ThriftClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);

    /** The index manager. */
    private IndexManager indexManager;

    /** The reader. */
    private EntityReader reader;

    /** Configure schema manager. */
    private SchemaManager schemaManager;

    /** Property reader. */
    private PropertyReader propertyReader;

    @Override
    public SchemaManager getSchemaManager()
    {
        if (schemaManager == null)
        {
            schemaManager = new CassandraSchemaManager(PelopsClientFactory.class.getName());
        }
        return schemaManager;
    }

    @Override
    public void destroy()
    {
        indexManager.close();
        getSchemaManager().dropSchema();
    }

    @Override
    public void initialize()
    {

        logger.info("Initializing Threadsafe Indexmanager. Is it really threadsafe?");

        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));

        reader = new CassandraEntityReader();
        propertyReader = new CassandraPropertyReader();
        propertyReader.read(getPersistenceUnit());
    }

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
            Cluster cluster = new Cluster(contactNodes, new IConnection.Config(Integer.parseInt(defaultPort), true, -1,
                    getAuthenticationRequest(props)), false);

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
        return new ThriftClient(indexManager, reader, persistenceUnit);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }
    
    /**
     * If userName and password provided, Method prepares for
     * AuthenticationRequest.
     * 
     * @param props
     *            properties
     * 
     * @return simple authenticator request. returns null if userName/password
     *         are not provided.
     * 
     */
    private SimpleConnectionAuthenticator getAuthenticationRequest(Properties props)
    {
        String userName = (String) props.get(PersistenceProperties.KUNDERA_USERNAME);
        String password = (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);

        SimpleConnectionAuthenticator authenticator = null;
        if (userName != null || password != null)
        {
            authenticator = new SimpleConnectionAuthenticator(userName, password);
        }
        return authenticator;
    }

}
