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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.mongodb.DB;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
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

        List<ServerAddress> addrs = new ArrayList<>();

        MongoClient mongo = null;
        try
        {
            mongo = onSetMongoServerProperties(contactNode, defaultPort, poolSize, addrs,
                    MongoDBUtils.fetchCredentials(props, externalProperties));

            logger.info("Connected to mongodb at " + contactNode + " on port " + defaultPort);
        }
        catch (NumberFormatException e)
        {
            logger.error("Invalid format for MONGODB port, Unale to connect!" + "; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }
        catch (MongoException e)
        {
            logger.error("Unable to connect to MONGODB; Caused by:" + e.getMessage());
            throw new ClientLoaderException(e);
        }
        return mongo.getDB(keyspace);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#onValidation(java.lang
     * .String, java.lang.String)
     */
    @Override
    protected void onValidation(final String contactNode, final String defaultPort)
    {
        if (contactNode != null)
        {
            // allow configuration as comma-separated list of host:port
            // addresses without the default port
            boolean allAddressesHaveHostAndPort = true;

            for (String node : contactNode.split(Constants.COMMA))
            {
                if (StringUtils.countMatches(node, Constants.COLON) == 1)
                {
                    // node is given with hostname and port
                    // count == 1 is to exclude IPv6 addresses
                    if (StringUtils.isNumeric(node.split(Constants.COLON)[1]))
                    {
                        continue;
                    }
                }

                allAddressesHaveHostAndPort = false;
                break;
            }

            if (allAddressesHaveHostAndPort)
            {
                return;
            }
        }

        // fall back to the generic validation which requires the default port
        // to be set
        super.onValidation(contactNode, defaultPort);
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
            List<ServerAddress> addrs, List<MongoCredential> credentials)
    {
        MongoClientOptions mo;
        MongoDBSchemaMetadata metadata = MongoDBPropertyReader.msmd;
        ClientProperties cp = metadata != null ? metadata.getClientProperties() : null;
        Properties propsFromCp = null;
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
            }

            propsFromCp = dataStore != null && dataStore.getConnection() != null ? dataStore.getConnection()
                    .getProperties() : null;
        }
        else
        {
            for (String node : contactNode.split(Constants.COMMA))
            {
                if (StringUtils.countMatches(node, Constants.COLON) == 1)
                {
                    // node is given with hostname and port
                    // count == 1 is to exclude IPv6 addresses
                    String host = node.split(":")[0];
                    int port = Integer.parseInt(node.split(Constants.COLON)[1]);

                    addrs.add(new ServerAddress(host.trim(), port));
                }
                else
                {
                    addrs.add(new ServerAddress(node.trim(), Integer.parseInt(defaultPort.trim())));
                }
            }
        }

        MongoClientOptions.Builder b = new PopulateMongoOptions(propsFromCp, externalProperties).prepareBuilder();
        mo = b.build();

        if (mo.getConnectionsPerHost() <= 0 && !StringUtils.isEmpty(poolSize))
        {
            mo = b.connectionsPerHost(Integer.parseInt(poolSize)).build();
        }

        return new MongoClient(addrs, credentials, mo);
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

        /** The Constant INSTANCE_CREATION_EXCEPTION. */
        private static final String INSTANCE_CREATION_EXCEPTION = "Cannot create instace from property";

        /** The logger. */
        private static Logger logger = LoggerFactory.getLogger(PopulateMongoOptions.class);

        /** The client properties. */
        private final Properties clientProperties;

        /** The external properties. */
        private final Map<String, ?> externalProperties;

        /**
         * Constructor.
         * 
         * @param clientProperties
         *            the properties from the 'kundera.client.property' file
         * @param externalProperties
         *            the external properties of the client factory
         */
        public PopulateMongoOptions(final Properties clientProperties, final Map<String, ?> externalProperties)
        {
            this.clientProperties = clientProperties;
            this.externalProperties = externalProperties;
        }

        /**
         * Prepare a mongo options builder.
         * 
         * @return the mongo client options. builder
         */
        public MongoClientOptions.Builder prepareBuilder()
        {
            MongoClientOptions.Builder builder = MongoClientOptions.builder();

            try
            {
                /*
                 * if value of SAFE is provided in client properties. Then it is
                 * given preference over other parameters values like W,
                 * W_TIME_OUT, FSYNC, J
                 * 
                 * So, whether choose simply write concern SAFE or not. Or you
                 * can put values like W, W_TIME_OUT
                 */
                int w = getProperty(MongoDBConstants.W, 1, int.class);
                int wTimeOut = getProperty(MongoDBConstants.W_TIME_OUT, 0, int.class);

                boolean j = getProperty(MongoDBConstants.J, false, boolean.class);

                boolean fsync = getProperty(MongoDBConstants.FSYNC, false, boolean.class);

                if (hasProperty(MongoDBConstants.SAFE))
                {
                    if (getProperty(MongoDBConstants.SAFE, false, boolean.class))
                    {
                        builder.writeConcern(WriteConcern.SAFE);
                    }
                    else
                    {
                        builder.writeConcern(WriteConcern.NORMAL);
                    }
                }
                else
                {
                    builder.writeConcern(new WriteConcern(w, wTimeOut, fsync, j));
                }

                if (hasProperty(MongoDBConstants.DB_DECODER_FACTORY))
                {
                    builder.dbDecoderFactory(getProperty(MongoDBConstants.DB_DECODER_FACTORY, DBDecoderFactory.class));
                }
                if (hasProperty(MongoDBConstants.DB_ENCODER_FACTORY))
                {
                    builder.dbEncoderFactory(getProperty(MongoDBConstants.DB_ENCODER_FACTORY, DBEncoderFactory.class));
                }
                if (hasProperty(MongoDBConstants.SOCKET_FACTORY))
                {
                    builder.socketFactory(getProperty(MongoDBConstants.SOCKET_FACTORY, SocketFactory.class));
                }
                if (hasProperty(MongoDBConstants.CONNECTION_PER_HOST))
                {
                    builder.connectionsPerHost(getProperty(MongoDBConstants.CONNECTION_PER_HOST, int.class));
                }
                if (hasProperty(MongoDBConstants.CONNECT_TIME_OUT))
                {
                    builder.connectTimeout(getProperty(MongoDBConstants.CONNECT_TIME_OUT, int.class));
                }
                if (hasProperty(MongoDBConstants.MAX_WAIT_TIME))
                {
                    builder.maxWaitTime(getProperty(MongoDBConstants.MAX_WAIT_TIME, int.class));
                }
                if (hasProperty(MongoDBConstants.TABCM))
                {
                    builder.threadsAllowedToBlockForConnectionMultiplier(getProperty(MongoDBConstants.TABCM, int.class));
                }
                if (hasProperty(MongoDBConstants.REPLICA_SET_NAME))
                {
                    builder.requiredReplicaSetName(getProperty(MongoDBConstants.REPLICA_SET_NAME, String.class));
                }
            }
            catch (NumberFormatException nfe)
            {
                logger.error("Error while setting mongo properties, caused by :" + nfe);
                throw new NumberFormatException("Error while setting mongo properties, caused by :" + nfe);
            }

            return builder;
        }

        /**
         * Checks for property.
         * 
         * @param key
         *            the key
         * @return true, if successful
         */
        private boolean hasProperty(String key)
        {
            boolean result = false;

            if (externalProperties != null)
            {
                result = externalProperties.containsKey(key);
            }

            if (!result && clientProperties != null)
            {
                result = clientProperties.containsKey(key);
            }

            return result;
        }

        /**
         * Gets the property.
         * 
         * @param <T>
         *            the generic type
         * @param key
         *            the key
         * @param targetClass
         *            the target class
         * @return the property
         */
        private <T> T getProperty(String key, Class<T> targetClass)
        {
            return getProperty(key, null, targetClass);
        }

        /**
         * Gets the property.
         * 
         * @param <T>
         *            the generic type
         * @param key
         *            the key
         * @param defaultValue
         *            the default value
         * @param targetClass
         *            the target class
         * @return the property
         */
        private <T> T getProperty(String key, T defaultValue, Class<T> targetClass)
        {
            T value = null;

            if (externalProperties != null)
            {
                value = instanceFromProperty(externalProperties.get(key), targetClass);
            }

            if (value == null && clientProperties != null)
            {
                value = instanceFromProperty(clientProperties.get(key), targetClass);
            }

            if (value != null)
            {
                return value;
            }
            else
            {
                return defaultValue;
            }
        }

        /**
         * Instance from property.
         * 
         * @param <T>
         *            the generic type
         * @param property
         *            the property
         * @param targetClass
         *            the target class
         * @return the t
         */
        private <T> T instanceFromProperty(Object property, Class<T> targetClass)
        {
            if (property == null)
            {
                return null;
            }

            if (targetClass.isInstance(property))
            {
                return (T) property;
            }

            if (property instanceof String)
            {
                String target = property.toString().trim();

                // try primitive parsers
                if (Integer.class.equals(targetClass) || int.class.equals(targetClass))
                {
                    return (T) Integer.valueOf(target);
                }
                else if (Long.class.equals(targetClass) || long.class.equals(targetClass))
                {
                    return (T) Long.valueOf(target);
                }
                else if (Boolean.class.equals(targetClass) || boolean.class.equals(targetClass))
                {
                    return (T) Boolean.valueOf(target);
                }

                // try instantiating from a class
                try
                {
                    Class<?> clazz = Class.forName(target).asSubclass(targetClass);
                    return (T) KunderaCoreUtils.createNewInstance(clazz);
                }
                catch (ClassNotFoundException ex)
                {
                    // this wasn't a class then in the configuration
                }

                // try accessing a static member
                int lastDot = target.lastIndexOf('.');
                if (lastDot > 0)
                {
                    String className = target.substring(0, lastDot);
                    String memberName = target.substring(lastDot + 1);

                    try
                    {
                        Class<?> clazz = Class.forName(className);

                        if (memberName.contains(Constants.OPEN_ROUND_BRACKET))
                        {
                            // static method without arguments - e.g.
                            // javax.net.ssl.SSLSocketFactory.getDefault()
                            String methodName = memberName.substring(0, memberName.indexOf('('));

                            Method method = clazz.getMethod(methodName);
                            return (T) method.invoke(null);
                        }
                        else
                        {
                            // static field - e.g.
                            // com.mongodb.LazyDBDecoder.FACTORY
                            Field field = clazz.getField(memberName);
                            return (T) field.get(null);
                        }
                    }
                    catch (ClassNotFoundException ex)
                    {
                        logger.error("Parent class not found", ex);
                        throw new KunderaException(INSTANCE_CREATION_EXCEPTION, ex);
                    }
                    catch (NoSuchMethodException ex)
                    {
                        logger.error("Static method not found", ex);
                        throw new KunderaException(INSTANCE_CREATION_EXCEPTION, ex);
                    }
                    catch (NoSuchFieldException ex)
                    {
                        logger.error("Static field not found", ex);
                        throw new KunderaException(INSTANCE_CREATION_EXCEPTION, ex);
                    }
                    catch (IllegalAccessException ex)
                    {
                        logger.error("Static method or field was not accessible", ex);
                        throw new KunderaException(INSTANCE_CREATION_EXCEPTION, ex);
                    }
                    catch (InvocationTargetException ex)
                    {
                        logger.error("Static method invocation failed", ex);
                        throw new KunderaException(INSTANCE_CREATION_EXCEPTION, ex);
                    }
                }
            }

            return null;
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
