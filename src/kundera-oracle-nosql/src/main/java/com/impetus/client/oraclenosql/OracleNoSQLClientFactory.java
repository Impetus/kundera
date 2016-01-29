/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.PasswordCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.config.OracleNoSQLPropertyReader;
import com.impetus.client.oraclenosql.index.OracleNoSQLInvertedIndexer;
import com.impetus.client.oraclenosql.schemamanager.OracleNoSQLSchemaManager;
import com.impetus.client.oraclenosql.server.OracleNoSQLHostConfiguration;
import com.impetus.client.oraclenosql.server.OracleNoSQLHost;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;

/**
 * {@link ClientFactory} implementation for Oracle NOSQL database
 * 
 * @author amresh.singh
 */
public class OracleNoSQLClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(OracleNoSQLClientFactory.class);

    private HostConfiguration configuration;

    /** The kvstore db. */
    private KVStore kvStore;

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(puProperties);
            schemaManager = new OracleNoSQLSchemaManager(OracleNoSQLClientFactory.class.getName(), puProperties,
                    kunderaMetadata);
        }
        return schemaManager;
    }

    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        initializePropertyReader();
        setExternalProperties(puProperties);
        configuration = new OracleNoSQLHostConfiguration(externalProperties, OracleNoSQLPropertyReader.osmd,
                getPersistenceUnit(), kunderaMetadata);
        reader = new OracleNoSQLEntityReader(kunderaMetadata);
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        String indexerClass = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(getPersistenceUnit())
                .getProperties().getProperty(PersistenceProperties.KUNDERA_INDEXER_CLASS);

        Client client = new OracleNoSQLClient(this, reader, indexManager, kvStore, externalProperties,
                getPersistenceUnit(), kunderaMetadata);
        populateIndexer(indexerClass, client);

        return client;
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    public void destroy()
    {
        indexManager.close();
        if (schemaManager != null)
        {
            getSchemaManager(externalProperties).dropSchema();
        }
        schemaManager = null;
        externalProperties = null;

        if (kvStore != null)
        {
            logger.info("Closing connection to kvStore.");
            kvStore.close();
            logger.info("Closed connection to kvStore.");
        }
        else
        {
            logger.warn("Can't close connection to kvStore, it was already disconnected");
        }
    }

    @Override
    protected Object createPoolOrConnection()
    {
        kvStore = getConnection();

        return kvStore;
    }

    /**
     * Populates {@link Indexer} into {@link IndexManager}
     * 
     * @param indexerClass
     * @param client
     */
    private void populateIndexer(String indexerClass, Client client)
    {
        if (indexerClass != null && indexerClass.equals(OracleNoSQLInvertedIndexer.class.getName()))
        {
            ((OracleNoSQLInvertedIndexer) indexManager.getIndexer()).setKvStore(kvStore);
            ((OracleNoSQLInvertedIndexer) indexManager.getIndexer()).setHandler(((OracleNoSQLClient) client)
                    .getHandler());
        }
    }

    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new OracleNoSQLPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    private KVStore getConnection()
    {

        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String hostName = null;
        String defaultPort = null;
        String storeName = null;
        String poolSize = null;
        String userName = null;
        String password = null;
        

        if (externalProperties != null)
        {
            hostName = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            defaultPort = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            storeName = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            poolSize = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }

        if (hostName == null)
        {
            hostName = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (defaultPort == null)
        {
            defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }
        // keyspace is keystore
        if (storeName == null)
        {
            storeName = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        if (poolSize == null)
        {
            poolSize = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        String[] hosts = new String[configuration.getHosts().size()];
        int count = 0;

        for (Host host : configuration.getHosts())
        {
            hosts[count] = host.getHost() + ":" + host.getPort();
            count++;
        }
        
        KVStoreConfig kconfig = new KVStoreConfig(storeName, hosts);
        try
        {

            if (!((OracleNoSQLHostConfiguration)configuration).getSecurityProps().isEmpty())
            {
                
                kconfig.setSecurityProperties(((OracleNoSQLHostConfiguration)configuration).getSecurityProps());
                
            }
            
            if (userName != null && password != null)
            {
                return KVStoreFactory
                        .getStore(kconfig, new PasswordCredentials(userName, password.toCharArray()), null);
            }
        }
        catch (AuthenticationFailureException afe)
        {
            /*
             * Could potentially retry the login, possibly with different
             * credentials, but in this simple example, we just fail the
             * attempt.
             */
            throw new KunderaException("Could not authorize the client connection " + afe.getMessage());

        }

        return KVStoreFactory.getStore(kconfig);
    }


    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}