package com.impetus.client.mongodb;

import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;

public class MongoDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(MongoDBClientFactory.class);

    IndexManager indexManager;

    private DB mongoDB;

    private EntityReader reader;

    @Override
    protected void initializeClient()
    {
        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));
        reader = new MongoEntityReader();
    }

    @Override
    protected Object createPoolOrConnection()
    {
        // TODO implement pool
        mongoDB = getConnection();
        return mongoDB;
    }

    @Override
    protected Client instantiateClient()
    {
        // TODO To change this one pool is implemented

        return new MongoDBClient(mongoDB, indexManager, reader);
    }

    private DB getConnection()
    {

        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNode = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        String poolSize = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);

        Mongo mongo = null;
        logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
        try
        {
            mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));

            if (!StringUtils.isEmpty(poolSize))
            {
                MongoOptions mo = mongo.getMongoOptions();
                mo.connectionsPerHost = Integer.parseInt(poolSize);
            }

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
        indexManager.close();
        if (mongoDB != null)
        {
            logger.info("Closing connection to mongodb.");
            mongoDB.getMongo().close();
            logger.info("Closed connection to mongodb.");
        }
        else
        {
            logger.warn("Can't close connection to MONGODB, it was already disconnected");
        }
    }
}
