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

import java.util.Map;
import java.util.Properties;

import net.dataforte.cassandra.pool.ConnectionPool;
import net.dataforte.cassandra.pool.PoolConfiguration;
import net.dataforte.cassandra.pool.PoolProperties;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * A factory of {@link ThriftClient} Currently it uses Pelops for Connection
 * pooling. Need to replace it with our own pooling. Most of the code is
 * borrowed from {@link PelopsClientFactory}
 * 
 * @author amresh.singh
 */
public class ThriftClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);

    /** Cassandra pool */
    private ConnectionPool pool;

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new CassandraSchemaManager(ThriftClientFactory.class.getName(), externalProperty);
        }
        return schemaManager;
    }

    /**
     * 
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new CassandraPropertyReader();
            propertyReader.read(getPersistenceUnit());
        }
    }

    @Override
    public void destroy()
    {
        if (indexManager != null)
        {
            indexManager.close();
        }
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
        schemaManager = null;
        externalProperties = null;
        // Pelops.shutdown();
        // Pelops.removePool(PelopsUtils.generatePoolName(getPersistenceUnit()));
    }

    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new CassandraEntityReader();
        initializePropertyReader();
        setExternalProperties(externalProperty);
    }

    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNodes = null;
        String defaultPort = null;
        String keyspace = null;
        if (externalProperties != null)
        {
            contactNodes = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            defaultPort = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        if (contactNodes == null)
        {
            contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (defaultPort == null)
        {
            defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }
        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        onValidation(contactNodes, defaultPort);

        PoolConfiguration prop = new PoolProperties();
        prop.setHost(contactNodes);
        prop.setPort(Integer.parseInt(defaultPort));
        prop.setKeySpace(keyspace);

        PelopsUtils.setPoolConfigPolicy(persistenceUnitMetadata, prop, externalProperties);
        try
        {
            if(logger.isInfoEnabled())
            {
                logger.info("Initializing connection for keyspace {},host {},port {}", keyspace,contactNodes,defaultPort);
            }
            
            pool = new ConnectionPool(prop);
        }
        catch (TException e)
        {
            logger.error("Error during creating poo, Caused by: .", e);
            throw new ClientLoaderException(e);
        }
        return null;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new ThriftClient(indexManager, reader, persistenceUnit, pool, externalProperties);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }
}
