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
        reader = new MongoEntityReader(kunderaMetadata);
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
        return new MongoDBClient(mongoDB, indexManager, reader, persistenceUnit, externalProperties, clientMetadata, kunderaMetadata);
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    private DB getConnection()
    {

        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata()
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

        onValidation(contactNode, defaultPort);

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
                    addrs.add(new ServerAddress(server.getHost().trim(), Integer.parseInt(server.getPort().trim())));
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
        else
        {
            logger.info("Connecting to mongodb at " + contactNode + " on port " + defaultPort);
            mongo = new Mongo(contactNode, Integer.parseInt(defaultPort));
            mo = mongo.getMongoOptions();
        }
        // setting server property.

        if (mo.getConnectionsPerHost() <= 0 && !StringUtils.isEmpty(poolSize))
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

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new MongoDBSchemaManager(MongoDBClientFactory.class.getName(), externalProperty, kunderaMetadata);
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
            propertyReader = new MongoDBPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
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
                        mo.setConnectionsPerHost(Integer.parseInt((String) props
                                .get(MongoDBConstants.CONNECTION_PER_HOST)));
                    }

                    if (props.get(MongoDBConstants.CONNECT_TIME_OUT) != null)
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

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}
