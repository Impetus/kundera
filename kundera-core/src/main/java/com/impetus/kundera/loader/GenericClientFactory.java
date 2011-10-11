package com.impetus.kundera.loader;

import org.apache.log4j.Logger;

import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

// Client Loaders are more of 
public abstract class GenericClientFactory implements Loader
{
    /** The logger. */
    private static Logger logger = Logger.getLogger(KunderaPersistence.class);

    private Client client;

    private String persistenceUnit;

    private Object connectionPoolOrConnection;

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

    protected void loadClientMetadata()
    {
        ClientMetadata clientMetadata = new ClientMetadata();

        if (KunderaMetadata.getInstance().getClientMetadata(persistenceUnit) == null)
            KunderaMetadata.getInstance().addClientMetadata(persistenceUnit, clientMetadata);
    }

    protected abstract void initialize();

    protected abstract Object createPoolOrConnection();

    public Client getClientInstance()
    {
        // if threadsafe recycle the same single instance; if not create a new
        // instance

        if (isClientThreadSafe())
        {
            logger.info("Returning threadsafe used client instance for persistence unit : " + persistenceUnit);
            if (client == null)
            {
                client = instantiateClient();
            }

        }
        else
        {
            logger.info("Returning fresh client instance for persistence unit : " + persistenceUnit);
            client = instantiateClient();
        }

        // Construct Client using persistenceunit
        client.setPersistenceUnit(persistenceUnit);
        return client;

    }

    protected abstract Client instantiateClient();

    protected abstract boolean isClientThreadSafe();

    protected String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    protected Object getConnectionPoolOrConnection()
    {
        return connectionPoolOrConnection;
    }

    private void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }
}
