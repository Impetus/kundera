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

/**
 * A factory for creating MongoDBClient objects.
 */
public class MongoDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(MongoDBClientFactory.class);

    /** The index manager. */
    IndexManager indexManager;

    /** The mongo db. */
    private DB mongoDB;

    /** The reader. */
    private EntityReader reader;

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
     */
    @Override
    protected void initializeClient()
    {
        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));
        reader = new MongoEntityReader();
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
     */
    @Override
    protected Object createPoolOrConnection()
    {
        // TODO implement pool
        mongoDB = getConnection();
        return mongoDB;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#instantiateClient()
     */
    @Override
    protected Client instantiateClient()
    {
        // TODO To change this one pool is implemented

        return new MongoDBClient(mongoDB, indexManager, reader);
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
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

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#isClientThreadSafe()
     */
    @Override
    protected boolean isClientThreadSafe()
    {
        return false;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
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
