/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.spark.client;

import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.spark.client.SparkPropertyReader.SparkSchemaMetadata;
import com.impetus.spark.query.SparkEntityReader;

/**
 * A factory for creating SparkClient objects.
 * 
 * @author: karthikp.manchala
 */
public class SparkClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(SparkClientFactory.class);

    /** The sparkconf. */
    private SparkConf sparkconf;

    /** The spark context. */
    private SparkContext sparkContext;

    /** The sql context. */
    private HiveContext sqlContext;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
   

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        reader = new SparkEntityReader(kunderaMetadata);
        setExternalProperties(puProperties);
        initializePropertyReader();
        PersistenceUnitMetadata pum = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                getPersistenceUnit());
        sparkconf = new SparkConf(true);
        configureClientProperties(pum);
        sparkContext = new SparkContext(sparkconf);
        sqlContext = new HiveContext(sparkContext);
    }

    /**
     * Configure client properties.
     * 
     * @param puMetadata
     *            the pu metadata
     */
    private void configureClientProperties(PersistenceUnitMetadata puMetadata)
    {
        SparkSchemaMetadata metadata = SparkPropertyReader.ssmd;
        ClientProperties cp = metadata != null ? metadata.getClientProperties() : null;
        if (cp != null)
        {
            DataStore dataStore = metadata != null ? metadata.getDataStore(puMetadata.getProperty("kundera.client"))
                    : null;
            List<Server> servers = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getServers() : null;

            if (servers!=null && !servers.isEmpty())
            {
                Server server = servers.get(0);
                sparkconf.set("hostname", server.getHost());
                sparkconf.set("portname", server.getPort());
            }

            Connection conn = dataStore.getConnection();

            Properties props = conn.getProperties();

            Enumeration e = props.propertyNames();

            while (e.hasMoreElements())
            {
                String key = (String) e.nextElement();
                sparkconf.set(key, props.getProperty(key));
            }
        }
    }

    /**
     * Initialize property reader.
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new SparkPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
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
        // TODO Auto-generated method stub
        return null;
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
        
        return new SparkClient(kunderaMetadata, reader, externalProperties, persistenceUnit, sparkContext, sqlContext);
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
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer
     * (java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        // TODO Auto-generated method stub

    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        indexManager.close();
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
        if (sparkContext != null)
        {
            logger.info("Closing connection to spark.");
            sparkContext.stop();
            logger.info("Closed connection to spark.");
        }
        else
        {
            logger.warn("Can't close connection to Spark, it was already disconnected");
        }
        externalProperties = null;
        schemaManager = null;
    }


}