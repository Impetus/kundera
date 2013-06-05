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

import java.util.Map;
import java.util.Properties;

import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * A factory for creating PelopsCliobjects.
 */
public class PelopsClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(PelopsClientFactory.class);

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
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit(), externalProperties);

        if (Pelops.getDbConnPool(poolName) == null)
        {
            onValidation(contactNodes, defaultPort);

            Cluster cluster = new Cluster(contactNodes, new IConnection.Config(Integer.parseInt(defaultPort), true, -1,
                    PelopsUtils.getAuthenticationRequest(props)), false);

            Policy policy = PelopsUtils.getPoolConfigPolicy(persistenceUnitMetadata, externalProperties);

            // Add pool with specified policy. null means default operand
            // policy.
            
            if(logger.isInfoEnabled())
            {
                logger.info("Initializing connection pool for keyspace {0}, host {1},port {2}.", keyspace,contactNodes,defaultPort);
            }
            
            Pelops.addPool(poolName, cluster, keyspace, policy, null);

        }
        // TODO return a thrift pool
        return null;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        if(logger.isInfoEnabled())
        {
            logger.info("Initializing pelops client for persistence unit {0}", persistenceUnit);
        }

        return new PelopsClient(indexManager, reader, persistenceUnit, externalProperties);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
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
        // Pelops.shutdown();
        // Pelops.removePool(PelopsUtils.generatePoolName(getPersistenceUnit(),
        // externalProperties));
        externalProperties = null;
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new CassandraSchemaManager(PelopsClientFactory.class.getName(), externalProperty);
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
}