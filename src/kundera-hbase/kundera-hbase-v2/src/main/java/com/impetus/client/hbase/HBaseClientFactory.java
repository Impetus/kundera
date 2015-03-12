/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.config.HBasePropertyReader;
import com.impetus.client.hbase.schemamanager.HBaseSchemaManager;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * HBaseClientFactory, instantiates client for HBase
 */
/**
 * @author Devender Yadav
 * 
 */
public class HBaseClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(HBaseClientFactory.class);

    /** The conf. */
    private Configuration conf;

    private org.apache.hadoop.hbase.client.Connection connection;

    private static final String DEFAULT_ZOOKEEPER_PORT = "2181";

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        setExternalProperties(externalProperty);
        initializePropertyReader();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                getPersistenceUnit());

        String node = null;
        String port = null;
        String poolSize = null;
        if (externalProperty != null)
        {
            node = (String) externalProperty.get(PersistenceProperties.KUNDERA_NODES);
            port = (String) externalProperty.get(PersistenceProperties.KUNDERA_PORT);
            poolSize = (String) externalProperty.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        if (node == null)
        {
            node = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_NODES);
        }
        if (port == null)
        {
            port = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_PORT);
        }
        if (poolSize == null)
        {
            poolSize = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        onValidation(node, port);

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", node + ":" + port);

        Connection conn = HBasePropertyReader.hsmd.getDataStore() != null ? HBasePropertyReader.hsmd.getDataStore()
                .getConnection() : null;
        if (conn != null && conn.getProperties() != null)
        {
            String zookeeperHost = conn.getProperties().getProperty("hbase.zookeeper.quorum").trim();
            String zookeeperPort = conn.getProperties().getProperty("hbase.zookeeper.property.clientPort").trim();
            hadoopConf.set("hbase.zookeeper.quorum", zookeeperHost != null ? zookeeperHost : node);
            hadoopConf.set("hbase.zookeeper.property.clientPort", zookeeperPort != null ? zookeeperPort
                    : DEFAULT_ZOOKEEPER_PORT);
        }
        else
        {
            hadoopConf.set("hbase.zookeeper.quorum", node);
            hadoopConf.set("hbase.zookeeper.property.clientPort", DEFAULT_ZOOKEEPER_PORT);
        }
        conf = HBaseConfiguration.create(hadoopConf);
        reader = new HBaseEntityReader(kunderaMetadata);
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
        try
        {
            this.connection = ConnectionFactory.createConnection(conf);
            return connection;
        }
        catch (IOException e)
        {
            throw new KunderaException("Connection could not be established", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java
     * .lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new HBaseClient(indexManager, conf, connection, reader, persistenceUnit, externalProperties,
                clientMetadata, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        try
        {
            if (schemaManager != null)
            {
                schemaManager.dropSchema();
            }
            externalProperties = null;
            schemaManager = null;
            connection.close();

        }
        catch (IOException e)
        {
            throw new KunderaException("connection already closed", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        setExternalProperties(externalProperty);
        if (schemaManager == null)
        {
            initializePropertyReader();
            schemaManager = new HBaseSchemaManager(HBaseClientFactory.class.getName(), externalProperty,
                    kunderaMetadata);
        }
        return schemaManager;
    }

    /**
     * Initialize property reader.
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new HBasePropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer
     * (java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}