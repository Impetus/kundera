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
package com.impetus.kundera.loader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.index.IndexingConstants;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.policy.LoadBalancingPolicy;
import com.impetus.kundera.service.policy.RetryService;
import com.impetus.kundera.service.policy.RoundRobinBalancingPolicy;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * Abstract class to hold generic definitions for client factory
 * implementations.
 * 
 * @author vivek.mishra
 */
public abstract class GenericClientFactory implements ClientFactory, ClientLifeCycleManager
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(GenericClientFactory.class);

    /** The client. */
    private Client client;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The connection pool or connection. */
    private Object connectionPoolOrConnection;

    /** The reader. */
    protected EntityReader reader;

    /** Configure schema manager. */
    protected SchemaManager schemaManager;

    /** property reader instance */
    protected PropertyReader propertyReader;

    /** Holds persistence unit related property */
    protected Map<String, Object> externalProperties = new HashMap<String, Object>();

    /** Holds LoadBalancer instance **/
    protected LoadBalancingPolicy loadBalancingPolicy = new RoundRobinBalancingPolicy();

    /** Holds Instance of retry service */
    protected RetryService hostRetryService;

    /** Holds one pool instance per host */
    protected ConcurrentMap<Host, Object> hostPools = new ConcurrentHashMap<Host, Object>();

    /**
     * Holds reference to client metadata.
     */
    protected ClientMetadata clientMetadata;

    /** kundera metadata */
    protected KunderaMetadata kunderaMetadata;

    /** The index manager. */
    protected IndexManager indexManager = new IndexManager(null, kunderaMetadata);

    /**
     * Load.
     * 
     * @param persistenceUnit
     *            the persistence unit
     */
    @Override
    public void load(String persistenceUnit, Map<String, Object> puProperties)
    {
        setPersistenceUnit(persistenceUnit);

        // Load Client Specific Stuff
        logger.info("Loading client metadata for persistence unit : " + persistenceUnit);
        loadClientMetadata(puProperties);

        // initialize the client
        logger.info("Initializing client for persistence unit : " + persistenceUnit);
        initialize(puProperties);

        // Construct Pool
        logger.info("Constructing pool for persistence unit : " + persistenceUnit);
        connectionPoolOrConnection = createPoolOrConnection();
    }

    /**
     * Load client metadata.
     * 
     * @param puProperties
     */
    protected void loadClientMetadata(Map<String, Object> puProperties)
    {
        clientMetadata = new ClientMetadata();
        String luceneDirectoryPath = puProperties != null ? (String) puProperties
                .get(PersistenceProperties.KUNDERA_INDEX_HOME_DIR) : null;

        String indexerClass = puProperties != null ? (String) puProperties
                .get(PersistenceProperties.KUNDERA_INDEXER_CLASS) : null;
                
        String autoGenClass = puProperties != null ? (String) puProperties
                        .get(PersistenceProperties.KUNDERA_AUTO_GENERATOR_CLASS) : null;


        if (indexerClass == null)
        {
            indexerClass = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit)
                    .getProperties().getProperty(PersistenceProperties.KUNDERA_INDEXER_CLASS);
        }
        if (autoGenClass == null)
        {
            autoGenClass = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit)
                    .getProperties().getProperty(PersistenceProperties.KUNDERA_AUTO_GENERATOR_CLASS);
        }

        if (luceneDirectoryPath == null)
        {
            luceneDirectoryPath = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit)
                    .getProperty(PersistenceProperties.KUNDERA_INDEX_HOME_DIR);
        }
        
        if (autoGenClass != null) 
        {
            clientMetadata.setAutoGenImplementor(autoGenClass);    
        }

        // in case set empty via external property, means want to avoid lucene
        // directory set up.
        if (luceneDirectoryPath != null && !StringUtils.isEmpty(luceneDirectoryPath))
        {
            // Add client metadata
            clientMetadata.setLuceneIndexDir(luceneDirectoryPath);

            // Set Index Manager

            try
            {
                Method method = Class.forName(IndexingConstants.LUCENE_INDEXER).getDeclaredMethod("getInstance",
                        String.class);

                Indexer indexer = (Indexer) method.invoke(null, luceneDirectoryPath);
                indexManager = new IndexManager(indexer, kunderaMetadata);
            }
            catch (Exception e)
            {
                logger.error(
                        "Missing lucene from classpath. Please make sure those are available to load lucene directory {}!",
                        luceneDirectoryPath);
                throw new InvalidConfigurationException(e);
            }

            // indexManager = new IndexManager(LuceneIndexer.getInstance(new
            // StandardAnalyzer(Version.LUCENE_CURRENT),
            // luceneDirectoryPath));
        }
        else if (indexerClass != null)
        {
            try
            {
                Class<?> indexerClazz = Class.forName(indexerClass);
                Indexer indexer = (Indexer) indexerClazz.newInstance();
                indexManager = new IndexManager(indexer, kunderaMetadata);
                clientMetadata.setIndexImplementor(indexerClass);
            }
            catch (Exception cnfex)
            {
                logger.error("Error while initialzing indexer:" + indexerClass, cnfex);
                throw new KunderaException(cnfex);
            }
        }
        else
        {
            indexManager = new IndexManager(null, kunderaMetadata);
        }
        // if
        // (kunderaMetadata.getClientMetadata(persistenceUnit)
        // ==
        // null)
        // {
        // kunderaMetadata.addClientMetadata(persistenceUnit,
        // clientMetadata);
        // }
    }

    /**
     * Initialize client.
     * 
     * @param puProperties
     */
    public abstract void initialize(Map<String, Object> puProperties);

    /**
     * Creates a new GenericClient object.
     * 
     * @param externalProperties
     * 
     * @return the object
     */
    protected abstract Object createPoolOrConnection();

    /**
     * Gets the client instance.
     * 
     * @return the client instance
     */
    @Override
    public Client getClientInstance()
    {
        // if threadsafe recycle the same single instance; if not create a new
        // instance

        if (isThreadSafe())
        {
            logger.info("Returning threadsafe used client instance for persistence unit : " + persistenceUnit);
            if (client == null)
            {
                client = instantiateClient(persistenceUnit);
            }
        }
        else
        {
            logger.debug("Returning fresh client instance for persistence unit : " + persistenceUnit);
            // no need to hold a client reference.
            return instantiateClient(persistenceUnit);
        }

        return client;
    }

    /**
     * Instantiate client.
     * 
     * @return the client
     */
    protected abstract Client instantiateClient(String persistenceUnit);

    /**
     * Checks if is client thread safe.
     * 
     * @return true, if is client thread safe
     */
    public abstract boolean isThreadSafe();

    /**
     * Gets the persistence unit.
     * 
     * @return the persistence unit
     */
    protected String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /**
     * Gets the connection pool or connection.
     * 
     * @return the connection pool or connection
     */
    protected Object getConnectionPoolOrConnection()
    {
        return connectionPoolOrConnection;
    }

    /**
     * Sets the connection pool or connection.
     */
    protected void setConnectionPoolOrConnection(Object connectionPoolOrConnection)
    {
        this.connectionPoolOrConnection = connectionPoolOrConnection;
    }

    /**
     * Sets the persistence unit.
     * 
     * @param persistenceUnit
     *            the new persistence unit
     */
    private void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

    /**
     * Sets the persistence unit.
     * 
     * @param persistenceUnit
     *            the new persistence unit
     */
    protected void setKunderaMetadata(KunderaMetadata kunderaMetadata)
    {
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * @param puProperties
     */
    protected void setExternalProperties(Map<String, Object> puProperties)
    {
        if (puProperties != null)
        {
            this.externalProperties = puProperties;
        }
    }

    protected void onValidation(final String host, final String port)
    {
        if (host == null || !StringUtils.isNumeric(port) || port.isEmpty())
        {
            logger.error("Host or port should not be null / port should be numeric");
            throw new IllegalArgumentException("Host or port should not be null / port should be numeric");
        }
    }

    protected void unload()
    {
        if (client != null)
        {
            client.close();
            client = null;
        }
        externalProperties = null;
        hostPools.clear();
    }

    protected abstract void initializeLoadBalancer(String loadBalancingPolicyName);

    public ClientMetadata getClientMetadata()
    {
        return this.clientMetadata;
    }

    protected enum LoadBalancer
    {
        ROUNDROBIN, LEASTACTIVE;

        public static LoadBalancer getValue(String loadBalancename)
        {
            if (loadBalancename != null && loadBalancename.equalsIgnoreCase(ROUNDROBIN.name()))
            {
                return ROUNDROBIN;
            }
            else if (loadBalancename != null && loadBalancename.equalsIgnoreCase(LEASTACTIVE.name()))
            {
                return LEASTACTIVE;
            }
            else
            {
                logger.info("Using default load balancer {} . " + ROUNDROBIN.name());
                return ROUNDROBIN;
            }
        }
    }
}
