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
package com.impetus.client.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.SocketFactory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.config.MongoDBPropertyReader;
import com.impetus.client.mongodb.config.MongoDBPropertyReader.MongoDBSchemaMetadata;
import com.impetus.client.mongodb.schemamanager.MongoDBSchemaManager;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.DB;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * A factory for creating MongoDBClient objects.
 * 
 * @author Devender Yadav
 */
public class MongoDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(MongoDBClientFactory.class);

    /** The mongo db. */
    private DB mongoDB;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new MongoEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(externalProperty);
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
        mongoDB = getConnection();
        return mongoDB;
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
        return new MongoDBClient(mongoDB, indexManager, reader, persistenceUnit, externalProperties, clientMetadata,
                kunderaMetadata);
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    private DB getConnection()
    {

        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                getPersistenceUnit());

        Properties props = puMetadata.getProperties();
        String contactNode = null;
        String defaultPort = null;
        String keyspace = null;
        String poolSize = null;
        if (externalProperties != null)
        {
            contactNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            defaultPort = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            poolSize = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        if (contactNode == null)
        {
            contactNode = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (defaultPort == null)
        {
            defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }
        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        if (poolSize == null)
        {
            poolSize = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }

        onValidation(contactNode, defaultPort);

        List<ServerAddress> addrs = new ArrayList<ServerAddress>();

        MongoClient mongo = null;
        try
        {
            mongo = onSetMongoServerProperties(contactNode, defaultPort, poolSize, addrs);

            logger.info("Connected to mongodb at " + contactNode + " on port " + defaultPort);
        }
        catch (NumberFormatException e)
        {
            logger.error("Invalid format for MONGODB port, Unale to connect!" + "; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }
        catch (UnknownHostException e)
        {
            logger.error("Unable to connect to MONGODB at host " + contactNode + "; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }
        catch (MongoException e)
        {
            logger.error("Unable to connect to MONGODB; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }

        DB mongoDB = mongo.getDB(keyspace);

        try
        {
            MongoDBUtils.authenticate(props, externalProperties, mongoDB);
        }
        catch (ClientLoaderException e)
        {
            logger.error(e.getMessage());
            throw e;
        }
        logger.info("Connected to mongodb at " + contactNode + " on port " + defaultPort);
        return mongoDB;

    }

    /**
     * On set mongo server properties.
     * 
     * @param contactNode
     *            the contact node
     * @param defaultPort
     *            the default port
     * @param poolSize
     *            the pool size
     * @param addrs
     *            the addrs
     * @return the mongo client
     * @throws UnknownHostException
     *             the unknown host exception
     */
    private MongoClient onSetMongoServerProperties(String contactNode, String defaultPort, String poolSize,
            List<ServerAddress> addrs) throws UnknownHostException
    {
        MongoClient mongo = null;
        MongoClientOptions mo = null;
        MongoDBSchemaMetadata metadata = MongoDBPropertyReader.msmd;
        ClientProperties cp = metadata != null ? metadata.getClientProperties() : null;
        if (cp != null)
        {
            DataStore dataStore = metadata != null ? metadata.getDataStore() : null;
            List<Server> servers = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getServers() : null;
            if (servers != null && !servers.isEmpty())
            {
                for (Server server : servers)
                {
                    addrs.add(new ServerAddress(server.getHost().trim(), Integer.parseInt(server.getPort().trim())));
                }
                mongo = new MongoClient(addrs);
            }
            else
            {
                logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
                // mongo = new MongoClient(contactNode,
                // Integer.parseInt(defaultPort));

            }
            mo = mongo.getMongoClientOptions();
            Properties p = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getProperties() : null;

            PopulateMongoOptions.populateMongoOptions(mo, p);

            mongo = new MongoClient(contactNode, mo);
        }
        else
        {
            logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
            mongo = new MongoClient(contactNode, Integer.parseInt(defaultPort));
            mo = mongo.getMongoClientOptions();
        }
        // setting server property.

        if (mo.getConnectionsPerHost() <= 0 && !StringUtils.isEmpty(poolSize))
        {
            mo = MongoClientOptions.builder().connectionsPerHost(Integer.parseInt(poolSize)).build();
            mongo.close();
            mongo = new MongoClient(contactNode, mo);
        }
        return mongo;
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
        indexManager.close();
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
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
        externalProperties = null;
        schemaManager = null;
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
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new MongoDBSchemaManager(MongoDBClientFactory.class.getName(), externalProperty,
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
            propertyReader = new MongoDBPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    /**
     * The Class PopulateMongoOptions.
     */
    public static class PopulateMongoOptions
    {

        /** The logger. */
        private static Logger logger = LoggerFactory.getLogger(PopulateMongoOptions.class);

        /**
         * Populate mongo options.
         * 
         * @param mo
         *            the mo
         * @param props
         *            the props
         */
        public static void populateMongoOptions(MongoClientOptions mo, Properties props)
        {
            if (props != null && mo != null)
            {
                try
                {
                    /*
                     * if value of SAFE is provided in client properties. Then
                     * it is given preference over other parameters values like
                     * W, W_TIME_OUT, FSYNC, J
                     * 
                     * So, whether choose simply write concern SAFE or not. Or
                     * you can put values like W, W_TIME_OUT
                     */
                    int w = props.get(MongoDBConstants.W) != null ? Integer.parseInt((String) props
                            .get(MongoDBConstants.W)) : 1;
                    int wTimeOut = props.get(MongoDBConstants.W_TIME_OUT) != null ? Integer.parseInt((String) props
                            .get(MongoDBConstants.W_TIME_OUT)) : 0;

                    boolean j = Boolean.parseBoolean((String) props.get(MongoDBConstants.J));

                    boolean fsync = Boolean.parseBoolean((String) props.get(MongoDBConstants.FSYNC));

                    if (props.get(MongoDBConstants.SAFE) != null)
                    {
                        if (Boolean.parseBoolean((String) props.get(MongoDBConstants.SAFE)))
                            MongoClientOptions.builder().writeConcern(WriteConcern.SAFE);
                        else
                            MongoClientOptions.builder().writeConcern(WriteConcern.NORMAL);
                    }
                    else
                    {
                        MongoClientOptions.builder().writeConcern(new WriteConcern(w, wTimeOut, fsync, j));
                    }

                    if (props.get(MongoDBConstants.DB_DECODER_FACTORY) != null)
                    {
                        MongoClientOptions.builder().dbDecoderFactory(
                                (DBDecoderFactory) props.get(MongoDBConstants.DB_DECODER_FACTORY));
                    }
                    if (props.get(MongoDBConstants.DB_ENCODER_FACTORY) != null)
                    {
                        MongoClientOptions.builder().dbEncoderFactory(
                                (DBEncoderFactory) props.get(MongoDBConstants.DB_ENCODER_FACTORY));
                    }
                    if (props.get(MongoDBConstants.SOCKET_FACTORY) != null)
                    {
                        MongoClientOptions.builder().socketFactory(
                                (SocketFactory) props.get(MongoDBConstants.SOCKET_FACTORY));
                    }

                    if (props.get(MongoDBConstants.AUTO_CONNECT_RETRY) != null)
                    {
                        MongoClientOptions.builder().autoConnectRetry(
                                (Boolean.parseBoolean((String) props.get(MongoDBConstants.AUTO_CONNECT_RETRY))));
                    }

                    if (props.get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY) != null)
                    {
                        MongoClientOptions.builder().maxAutoConnectRetryTime(
                                (Long.parseLong((String) props.get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY))));
                    }

                    if (props.get(MongoDBConstants.CONNECTION_PER_HOST) != null)
                    {
                        MongoClientOptions.builder().connectionsPerHost(
                                Integer.parseInt((String) props.get(MongoDBConstants.CONNECTION_PER_HOST)));
                    }

                    if (props.get(MongoDBConstants.CONNECT_TIME_OUT) != null)
                    {
                        MongoClientOptions.builder().connectTimeout(
                                Integer.parseInt((String) props.get(MongoDBConstants.CONNECT_TIME_OUT)));
                    }
                    if (props.get(MongoDBConstants.MAX_WAIT_TIME) != null)
                    {
                        MongoClientOptions.builder().maxWaitTime(
                                Integer.parseInt((String) props.get(MongoDBConstants.MAX_WAIT_TIME)));
                    }
                    if (props.get(MongoDBConstants.TABCM) != null)
                    {
                        MongoClientOptions.builder().threadsAllowedToBlockForConnectionMultiplier(
                                Integer.parseInt((String) props.get(MongoDBConstants.TABCM)));
                    }
                }
                catch (NumberFormatException nfe)
                {
                    logger.error("Error while setting mongo properties, caused by :" + nfe);
                    throw new NumberFormatException("Error while setting mongo properties, caused by :" + nfe);
                }
            }
            MongoClientOptions.builder().build();
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
