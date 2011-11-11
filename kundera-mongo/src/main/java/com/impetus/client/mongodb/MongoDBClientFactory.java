package com.impetus.client.mongodb;

import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = Logger.getLogger(MongoDBClientFactory.class);

    @Override
    protected void initializeClient()
    {
        // TODO Try to created table indexes over here

    }

    @Override
    protected Object createPoolOrConnection()
    {
        // TODO implement pool
        return null;
    }

    @Override
    protected Client instantiateClient()
    {
        // TODO To change this one pool is implemented
        DB mongoDB = getConnection();
        return new MongoDBClient(mongoDB);
    }

    private DB getConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNode = (String) props.get("kundera.nodes");
        String defaultPort = (String) props.get("kundera.port");
        String keyspace = (String) props.get("kundera.keyspace");

        Mongo mongo = null;
        logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
        try
        {
            mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
            logger.info("Connected to mongodb at " + contactNode + " on port " + defaultPort);
        }
        catch (NumberFormatException e)
        {
            logger.error("Invalid format for MONGODB port, Unale to connect!" + "; Details:" + e.getMessage());
        }
        catch (UnknownHostException e)
        {
            logger.error("Unable to connect to MONGODB at host " + contactNode + "; Details:" + e.getMessage());
        }
        catch (MongoException e)
        {
            logger.error("Unable to connect to MONGODB; Details:" + e.getMessage());
        }

        DB mongoDB = mongo.getDB(keyspace);
        return mongoDB;
    }

    @Override
    protected boolean isClientThreadSafe()
    {
        return false;
    }

    @Override
    public void unload(String... persistenceUnits)
    {
        // TODO Remove connection pool
    }
}
