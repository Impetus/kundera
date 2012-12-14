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
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.loader.KunderaAuthenticationException;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.DB;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

/**
 * A factory for creating MongoDBClient objects.
 */
public class MongoDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(MongoDBClientFactory.class);

    /** The mongo db. */
    private DB mongoDB;

    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new MongoEntityReader();
        initializePropertyReader();
        setExternalProperties(externalProperty);
    }

    @Override
    protected Object createPoolOrConnection()
    {
        mongoDB = getConnection();
        return mongoDB;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new MongoDBClient(mongoDB, indexManager, reader, persistenceUnit, externalProperties);
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    private DB getConnection()
    {

        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

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

        List<ServerAddress> addrs = new ArrayList<ServerAddress>();

        Mongo mongo = null;
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

        authenticate(props, mongoDB);
        logger.info("Connected to mongodb at " + contactNode + " on port " + defaultPort);
        return mongoDB;

    }

    /**
     * @param contactNode
     * @param defaultPort
     * @param poolSize
     * @param addrs
     * @return
     * @throws UnknownHostException
     */
    private Mongo onSetMongoServerProperties(String contactNode, String defaultPort, String poolSize,
            List<ServerAddress> addrs) throws UnknownHostException
    {
        Mongo mongo = null;
        MongoOptions mo = null;
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
                    addrs.add(new ServerAddress(server.getHost(), Integer.parseInt(server.getPort())));
                }
                mongo = new Mongo(addrs);
            }
            else
            {
                logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
                mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
            }
            mo = mongo.getMongoOptions();
            Properties p = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getProperties() : null;

            PopulateMongoOptions.populateMongoOptions(mo, p);
        }
        // else if (metadata != null && metadata.getConnections() != null &&
        // !metadata.getConnections().isEmpty())
        // {
        // addrs = new ArrayList<ServerAddress>();
        // for (MongoDBConnection connection : metadata.getConnections())
        // {
        // logger.info("Connecting to mongodb at " + connection.getHost() +
        // " on port " + connection.getPort());
        // addrs.add(new ServerAddress(connection.getHost(),
        // Integer.parseInt(connection.getPort())));
        // }
        // mongo = new Mongo(addrs);
        // mo = mongo.getMongoOptions();
        // mo.socketTimeout = metadata.getSocketTimeOut();
        // mongo.setReadPreference(metadata.getReadPreference());
        // }
        else
        {
            logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
            mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
        }
        // setting server property.

        if (!StringUtils.isEmpty(poolSize))
        {
            mo.connectionsPerHost = Integer.parseInt(poolSize);
        }
        return mongo;
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    public void destroy()
    {
        indexManager.close();
        if (schemaManager != null)
        {
            getSchemaManager(externalProperties).dropSchema();
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

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new MongoDBSchemaManager(MongoDBClientFactory.class.getName(), externalProperty);
        }
        return schemaManager;
    }

    /**
     * 
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new MongoDBPropertyReader();
            propertyReader.read(getPersistenceUnit());
        }
    }

    /**
     * Method to authenticate connection with mongodb. throws runtime error if:
     * a) userName and password, any one is not null. b) if authentication
     * fails.
     * 
     * 
     * @param props
     *            persistence properties.
     * @param mongoDB
     *            mongo db connection.
     */
    private void authenticate(Properties props, DB mongoDB)
    {
        String password = null;
        String userName = null;
        if (externalProperties != null)
        {
            userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        if (userName == null)
        {
            userName = (String) props.get(PersistenceProperties.KUNDERA_USERNAME);
        }
        if (password == null)
        {
            password = (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        boolean authenticate = true;
        String errMsg = null;
        if (userName != null && password != null)
        {
            authenticate = mongoDB.authenticate(userName, password.toCharArray());
        }
        else if ((userName != null && password == null) || (userName == null && password != null))
        {
            errMsg = "Invalid configuration provided for authentication, please specify both non-nullable 'kundera.username' and 'kundera.password' properties";
            logger.error(errMsg);
            throw new ClientLoaderException(errMsg);
        }

        if (!authenticate)
        {
            errMsg = "Authentication failed, invalid 'kundera.username' :" + userName + "and 'kundera.password' :"
                    + password + " provided";
            throw new KunderaAuthenticationException(errMsg);
        }
    }

    public static class PopulateMongoOptions
    {
        private static Logger logger = LoggerFactory.getLogger(PopulateMongoOptions.class);

        public static void populateMongoOptions(MongoOptions mo, Properties props)
        {
            if (props != null && mo != null)
            {
                if (props.get(MongoDBConstants.DB_DECODER_FACTORY) != null)
                {
                    mo.setDbDecoderFactory((DBDecoderFactory) props.get(MongoDBConstants.DB_DECODER_FACTORY));
                }
                if (props.get(MongoDBConstants.DB_ENCODER_FACTORY) != null)
                {
                    mo.setDbEncoderFactory((DBEncoderFactory) props.get(MongoDBConstants.DB_ENCODER_FACTORY));
                }

                mo.setAutoConnectRetry(Boolean.parseBoolean((String) props.get(MongoDBConstants.AUTO_CONNECT_RETRY)));
                mo.setFsync(Boolean.parseBoolean((String) props.get(MongoDBConstants.FSYNC)));
                mo.setJ(Boolean.parseBoolean((String) props.get(MongoDBConstants.J)));

                if (props.get(MongoDBConstants.SAFE) != null)
                {
                    mo.setSafe((Boolean) props.get(MongoDBConstants.SAFE));
                }
                if (props.get(MongoDBConstants.SOCKET_FACTORY) != null)
                {
                    mo.setSocketFactory((SocketFactory) props.get(MongoDBConstants.SOCKET_FACTORY));
                }

                try
                {
                    if (props.get(MongoDBConstants.CONNECTION_PER_HOST) != null)
                    {
                        mo.setConnectTimeout(Integer.parseInt((String) props.get(MongoDBConstants.CONNECT_TIME_OUT)));
                    }
                    if (props.get(MongoDBConstants.MAX_WAIT_TIME) != null)
                    {
                        mo.setMaxWaitTime(Integer.parseInt((String) props.get(MongoDBConstants.MAX_WAIT_TIME)));
                    }
                    if (props.get(MongoDBConstants.TABCM) != null)
                    {
                        mo.setThreadsAllowedToBlockForConnectionMultiplier(Integer.parseInt((String) props
                                .get(MongoDBConstants.TABCM)));
                    }
                    if (props.get(MongoDBConstants.W) != null)
                    {
                        mo.setW(Integer.parseInt((String) props.get(MongoDBConstants.W)));
                    }
                    if (props.get(MongoDBConstants.W_TIME_OUT) != null)
                    {
                        mo.setWtimeout(Integer.parseInt((String) props.get(MongoDBConstants.W_TIME_OUT)));
                    }
                    if (props.get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY) != null)
                    {
                        mo.setMaxAutoConnectRetryTime(Long.parseLong((String) props
                                .get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY)));
                    }
                }
                catch (NumberFormatException nfe)
                {
                    logger.warn("Error while setting mongo properties, caused by :" + nfe);
                }
            }
        }
    }
}
