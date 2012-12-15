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
        String contactNode = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        String password = (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);

        String maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        String maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
        String minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
        String maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        String txTimeOut = props.getProperty(PersistenceProperties.KUNDERA_TRANSACTION_TIMEOUT);

        JedisPoolConfig poolConfig = onPoolConfig(WHEN_EXHAUSTED_FAIL, maxActivePerNode, maxIdlePerNode,
                minIdlePerNode, maxTotal);

        // TODO: need to look for transaction timeout once transaction is
        // implemented.
        JedisPool pool = null;
        if (password != null)
        {
            pool = new JedisPool(poolConfig, contactNode, Integer.parseInt(defaultPort), txTimeOut != null
                    && StringUtils.isNumeric(txTimeOut) ? Integer.parseInt(txTimeOut) : 0, password);
        }
        else
        {
            pool = new JedisPool(poolConfig, contactNode, Integer.parseInt(defaultPort), txTimeOut != null
                    && StringUtils.isNumeric(txTimeOut) ? Integer.parseInt(txTimeOut) : 0);
        }

        return pool;
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
        return new RedisClient(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientFactory#getSchemaManager()
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
     {
        // TODO Auto-generated method stub
        return null;
    }

    Jedis getConnection()
    {
        return ((JedisPool) getConnectionPoolOrConnection()).getResource();
    }

    void releaseConnection(Jedis res)
    {
        ((JedisPool) getConnectionPoolOrConnection()).returnResource(res);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        if (getConnectionPoolOrConnection() != null)
        {
            ((JedisPool) getConnectionPoolOrConnection()).destroy();
        }

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

        if (maxTotal != null && StringUtils.isNumeric(maxTotal))
        {
            poolConfig.setMaxActive(Integer.parseInt(maxTotal));
        }
        if (maxActivePerNode != null && StringUtils.isNumeric(maxActivePerNode))
        {
            poolConfig.setWhenExhaustedAction(WHEN_EXHAUSTED_FAIL);
        }
        return poolConfig;
    }

}
