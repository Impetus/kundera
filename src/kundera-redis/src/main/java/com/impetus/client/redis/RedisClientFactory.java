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

package com.impetus.client.redis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Default client factory implementation for REDIS <a>redis.io</a>
 * 
 * @author vivek.mishra
 */
public class RedisClientFactory extends GenericClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClientFactory.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#initialize()
     */
    /**
     * Initialize redis client factory.
     */
    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        setExternalProperties(externalProperty);
        initializePropertyReader();
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
        logger.info("Initializing Redis connection pool");

        final byte WHEN_EXHAUSTED_FAIL = 0;

        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = puMetadata.getProperties();
        String contactNode = RedisPropertyReader.rsmd.getHost() != null ? RedisPropertyReader.rsmd.getHost()
                : (String) props.get(PersistenceProperties.KUNDERA_NODES);
        if (contactNode == null)
        {
            contactNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
        }
        String defaultPort = RedisPropertyReader.rsmd.getPort() != null ? RedisPropertyReader.rsmd.getPort()
                : (String) props.get(PersistenceProperties.KUNDERA_PORT);

        if (defaultPort == null)
        {
            defaultPort = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
        }

        String password = RedisPropertyReader.rsmd.getPassword() != null ? RedisPropertyReader.rsmd.getPassword()
                : (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);

        if (password == null)
        {
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }

        String maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);

        if (maxActivePerNode == null)
        {
            maxActivePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }

        String maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);

        if (maxIdlePerNode == null)
        {
            maxIdlePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
        }

        String minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);

        if (minIdlePerNode == null)
        {
            minIdlePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
        }

        String maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);

        if (maxTotal == null)
        {
            maxTotal = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        }

        String txTimeOut = props.getProperty(PersistenceProperties.KUNDERA_TRANSACTION_TIMEOUT);

        if (txTimeOut == null)
        {
            txTimeOut = (String) externalProperties.get(PersistenceProperties.KUNDERA_TRANSACTION_TIMEOUT);
        }

        JedisPoolConfig poolConfig = onPoolConfig(WHEN_EXHAUSTED_FAIL, maxActivePerNode, maxIdlePerNode,
                minIdlePerNode, maxTotal);

        JedisPool pool = null;
        onValidation(contactNode, defaultPort);

        if (poolConfig != null)
        {
            if (password != null)
            {
                pool = new JedisPool(poolConfig, contactNode, Integer.parseInt(defaultPort), txTimeOut != null
                        && StringUtils.isNumeric(txTimeOut) ? Integer.parseInt(txTimeOut) : -1, password);
            }
            else
            {
                pool = new JedisPool(poolConfig, contactNode, Integer.parseInt(defaultPort), txTimeOut != null
                        && StringUtils.isNumeric(txTimeOut) ? Integer.parseInt(txTimeOut) : -1);
            }

            return pool;
        }
        else
        {
            // Jedis connection = new Jedis(contactNode,
            // Integer.valueOf(defaultPort));

            // if (password != null)
            // {
            // // connection.auth(password);
            // }
            // connection.connect();
            // Connection to made available at the time of getConnection(). YCSB
            // performance fixes and ideally it is needed at that time only.
            // No need to cache it at factory level as needed to managed within
            // entity manager boundary!
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java
     * .lang.String)
     */
    @Override
    protected Client<RedisQuery> instantiateClient(String persistenceUnit)
    {
        logger.info("instantiating client instance");
        return new RedisClient(this, persistenceUnit);
    }

    Map<String, Object> getOverridenProperties()
    {
        return this.externalProperties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientFactory#getSchemaManager()
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        // if(logger.isDebugEnabled())
        logger.info("on close destroying connection pool");

        if (getConnectionPoolOrConnection() != null && getConnectionPoolOrConnection() instanceof JedisPool)
        {
            ((JedisPool) getConnectionPoolOrConnection()).destroy();
        }
        else if (getConnectionPoolOrConnection() != null && getConnectionPoolOrConnection() instanceof Jedis)
        {
            ((Jedis) getConnectionPoolOrConnection()).disconnect();
        }

    }

    /**
     * Retrieving connection from connection pool.
     * 
     * @return returns jedis instance.
     */
    Jedis getConnection()
    {
        if (logger.isDebugEnabled())
            logger.info("borrowing connection from pool");
        Object poolOrConnection = getConnectionPoolOrConnection();
        if (poolOrConnection != null && poolOrConnection instanceof JedisPool)
        {

            Jedis connection = ((JedisPool) getConnectionPoolOrConnection()).getResource();

            Map props = RedisPropertyReader.rsmd.getProperties();

            // set external xml properties.
            if (props != null)
            {
                // props.
                for (Object key : props.keySet())
                {
                    connection.configSet(key.toString(), props.get(key).toString());
                }
            }
            return connection;
        }
        else
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit());

            Properties props = puMetadata.getProperties();

            String contactNode = RedisPropertyReader.rsmd.getHost() != null ? RedisPropertyReader.rsmd.getHost()
                    : (String) props.get(PersistenceProperties.KUNDERA_NODES);
            String defaultPort = RedisPropertyReader.rsmd.getPort() != null ? RedisPropertyReader.rsmd.getPort()
                    : (String) props.get(PersistenceProperties.KUNDERA_PORT);
            String password = RedisPropertyReader.rsmd.getPassword() != null ? RedisPropertyReader.rsmd.getPassword()
                    : (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);

            if (defaultPort == null || !StringUtils.isNumeric(defaultPort))
            {
                throw new RuntimeException("Invalid port provided: " + defaultPort);
            }
            Jedis connection = new Jedis(contactNode, Integer.parseInt(defaultPort));

            if (password != null)
            {
                connection.auth(password);
            }
            connection.connect();
            return connection;
        }
    }

    /**
     * Release/return connection to pool.
     * 
     * @param res
     *            jedis resource
     */
    void releaseConnection(Jedis res)
    {
        if (logger.isDebugEnabled())
            logger.info("releasing connection from pool");
        Object poolOrConnection = getConnectionPoolOrConnection();
        if (poolOrConnection instanceof JedisPool)
        {
            ((JedisPool) poolOrConnection).returnResource(res);
        }
    }

    IndexManager getIndexManager()
    {
        return indexManager;
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
     * On pool config event.
     */
    private JedisPoolConfig onPoolConfig(final byte WHEN_EXHAUSTED_FAIL, String maxActivePerNode,
            String maxIdlePerNode, String minIdlePerNode, String maxTotal)
    {
        if (!StringUtils.isBlank(maxActivePerNode) && StringUtils.isNumeric(maxActivePerNode))
        {
            logger.info("configuring connection pool");

            JedisPoolConfig poolConfig = new JedisPoolConfig();

            if (maxActivePerNode != null && StringUtils.isNumeric(maxActivePerNode))
            {
                poolConfig.setMaxActive(Integer.valueOf(maxActivePerNode));
            }

            if (maxIdlePerNode != null && StringUtils.isNumeric(maxIdlePerNode))
            {
                poolConfig.setMaxIdle(Integer.valueOf(maxIdlePerNode));
            }
            if (minIdlePerNode != null && StringUtils.isNumeric(minIdlePerNode))
            {
                poolConfig.setMinIdle(Integer.parseInt(minIdlePerNode));
            }
            if (maxActivePerNode != null && StringUtils.isNumeric(maxActivePerNode))
            {
                poolConfig.setWhenExhaustedAction(WHEN_EXHAUSTED_FAIL);
            }
            return poolConfig;
        }

        return null;
    }

    /**
     * 
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new RedisPropertyReader(externalProperties);
            propertyReader.read(getPersistenceUnit());
        }
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}