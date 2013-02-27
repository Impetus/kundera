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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;

/**
 * Abstract class to hold generic definitions for client factory
 * implementations.
 * 
 * @vivek.mishra
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

    /** The index manager. */
    protected IndexManager indexManager;

    /** The reader. */
    protected EntityReader reader;

    /** Configure schema manager. */
    protected SchemaManager schemaManager;

    /** property reader instance */
    protected PropertyReader propertyReader;

    /** Holds persistence unit related property */
    protected Map<String, Object> externalProperties;

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
        if (KunderaMetadata.INSTANCE.getClientMetadata(persistenceUnit) == null)
        {
            ClientMetadata clientMetadata = new ClientMetadata();
            String luceneDirectoryPath = puProperties != null ? (String) puProperties
                    .get(PersistenceProperties.KUNDERA_INDEX_HOME_DIR) : null;
            if (luceneDirectoryPath == null)
            {
                luceneDirectoryPath = KunderaMetadata.INSTANCE.getApplicationMetadata()
                        .getPersistenceUnitMetadata(persistenceUnit)
                        .getProperty(PersistenceProperties.KUNDERA_INDEX_HOME_DIR);
            }

            // Add client metadata
            clientMetadata.setLuceneIndexDir(luceneDirectoryPath);
            KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);

            // Set Index Manager
            indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                    luceneDirectoryPath));

        }
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
            client = instantiateClient(persistenceUnit);
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
     * @param puProperties
     */
    protected void setExternalProperties(Map<String, Object> puProperties)
    {
        if (this.externalProperties == null)
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
}
