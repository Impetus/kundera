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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

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

    /**
     * Load.
     * 
     * @param persistenceUnit
     *            the persistence unit
     */
    @Override
    public void load(String persistenceUnit)
    {

        setPersistenceUnit(persistenceUnit);

        // Load Client Specific Stuff
        logger.info("Loading client metadata for persistence unit : " + persistenceUnit);
        loadClientMetadata();

        // initialize the client
        logger.info("Initializing client for persistence unit : " + persistenceUnit);
        initialize();

        // Construct Pool
        logger.info("Constructing pool for persistence unit : " + persistenceUnit);
        connectionPoolOrConnection = createPoolOrConnection();
    }

    /**
     * Load client metadata.
     */
    protected void loadClientMetadata()
    {
        if (KunderaMetadata.INSTANCE.getClientMetadata(persistenceUnit) == null)
        {
            ClientMetadata clientMetadata = new ClientMetadata();
            String secIndex = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getPersistenceUnitMetadata(persistenceUnit)
                    .getProperty(PersistenceProperties.KUNDERA_INDEX_HOME_DIR);
            clientMetadata.setLuceneIndexDir(secIndex);
            KunderaMetadata.INSTANCE.addClientMetadata(persistenceUnit, clientMetadata);
        }
    }

    /**
     * Initialize client.
     */
    public abstract void initialize();

    /**
     * Creates a new GenericClient object.
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
                client = instantiateClient();
            }

        }
        else
        {
            logger.debug("Returning fresh client instance for persistence unit : " + persistenceUnit);
            client = instantiateClient();
        }

        // Construct Client using persistenceunit
        client.setPersistenceUnit(persistenceUnit);
        return client;

    }

    /**
     * Instantiate client.
     * 
     * @return the client
     */
    protected abstract Client instantiateClient();

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
     * Sets the persistence unit.
     * 
     * @param persistenceUnit
     *            the new persistence unit
     */
    private void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }
}
