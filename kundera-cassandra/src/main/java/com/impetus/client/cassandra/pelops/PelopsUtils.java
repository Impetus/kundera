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
package com.impetus.client.cassandra.pelops;

import java.util.Map;
import java.util.Properties;

import net.dataforte.cassandra.pool.HostFailoverPolicy;
import net.dataforte.cassandra.pool.PoolConfiguration;

import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.SimpleConnectionAuthenticator;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class PelopsUtils.
 */
public class PelopsUtils
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(PelopsUtils.class);

    /**
     * Generate pool name.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     * @return the string
     */
    public static String generatePoolName(String persistenceUnit, Map<String, Object> puProperties)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadatata.getProperties();
        String contactNodes = null;
        String defaultPort = null;
        String keyspace = null;
        if (puProperties != null)
        {
            contactNodes = (String) puProperties.get(PersistenceProperties.KUNDERA_NODES);
            defaultPort = (String) puProperties.get(PersistenceProperties.KUNDERA_PORT);
            keyspace = (String) puProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        if (contactNodes == null)
        {
            contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (defaultPort == null)
        {
            defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }
        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        return generatePoolName(contactNodes, defaultPort, keyspace);
    }

    /**
     * Generate pool name.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param puProperties
     * @return the string
     */
    public static String generatePoolName(String node, String portAsString, String keyspace)
    {
        return node + ":" + portAsString + ":" + keyspace;
    }

    /**
     * Gets the pool config policy.
     * 
     * @param persistenceUnitMetadata
     *            the persistence unit metadata
     * @param puProperties
     * @return the pool config policy
     */
    public static Policy getPoolConfigPolicy(PersistenceUnitMetadata persistenceUnitMetadata,
            Map<String, Object> puProperties)
    {
        Policy policy = new Policy();

        Properties props = persistenceUnitMetadata.getProperties();
        String maxActivePerNode = null;
        String maxIdlePerNode = null;
        String minIdlePerNode = null;
        String maxTotal = null;
        if (puProperties != null)
        {
            maxActivePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            maxIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
            minIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
            maxTotal = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        }

        if (maxActivePerNode == null)
        {
            maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        if (maxIdlePerNode == null)
        {
            maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
        }
        if (minIdlePerNode == null)
        {
            minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
        }
        if (maxTotal == null)
        {
            maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        }
        try
        {
            if (!StringUtils.isEmpty(maxActivePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxActivePerNode));
            }

            if (!StringUtils.isEmpty(maxIdlePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxIdlePerNode));
            }

            if (!StringUtils.isEmpty(minIdlePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(minIdlePerNode));
            }

            if (!StringUtils.isEmpty(maxTotal))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxTotal));
            }
        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property for " + persistenceUnitMetadata.getPersistenceUnitName()
                    + " persistence unit couldn't be parsed. Default pool policy would be used");
            policy = null;
        }
        return policy;
    }

    /**
     * Gets the pool config policy.
     * 
     * @param persistenceUnitMetadata
     *            the persistence unit metadata
     * @param puProperties
     * @return the pool config policy
     */
    public static PoolConfiguration setPoolConfigPolicy(CassandraHost cassandraHost, PoolConfiguration prop)
    {
        int maxActivePerNode = cassandraHost.getMaxActive();
        int maxIdlePerNode = cassandraHost.getMaxIdle();
        int minIdlePerNode = cassandraHost.getMinIdle();
        int maxTotal = cassandraHost.getMaxTotal();
        boolean testOnBorrow = cassandraHost.isTestOnBorrow();
        boolean testWhileIdle = cassandraHost.isTestWhileIdle();
        boolean testOnConnect = cassandraHost.isTestOnConnect();
        boolean testOnReturn = cassandraHost.isTestOnReturn();
        int socketTimeOut = cassandraHost.getSocketTimeOut();
        HostFailoverPolicy paramHostFailoverPolicy = cassandraHost.getHostFailoverPolicy();
        if (maxActivePerNode > 0)
        {
            prop.setInitialSize(maxActivePerNode);
            prop.setMaxActive(maxActivePerNode);
        }
        if (maxIdlePerNode > 0)
        {
            prop.setMaxIdle(maxIdlePerNode);
        }
        if (minIdlePerNode > 0)
        {
            prop.setMinIdle(minIdlePerNode);
        }
        if (maxTotal > 0)
        {
            prop.setMaxActive(maxTotal);
        }
        prop.setSocketTimeout(socketTimeOut);
        prop.setTestOnBorrow(testOnBorrow);
        prop.setTestOnConnect(testOnConnect);
        prop.setTestOnReturn(testOnReturn);
        prop.setTestWhileIdle(testWhileIdle);
        prop.setFailoverPolicy(paramHostFailoverPolicy);
        return prop;
    }

    /**
     * If userName and password provided, Method prepares for
     * AuthenticationRequest.
     * 
     * @param props
     *            properties
     * 
     * @return simple authenticator request. returns null if userName/password
     *         are not provided.
     * 
     */
    public static SimpleConnectionAuthenticator getAuthenticationRequest(Properties props)
    {
        String userName = (String) props.get(PersistenceProperties.KUNDERA_USERNAME);
        String password = (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);

        SimpleConnectionAuthenticator authenticator = null;
        if (userName != null || password != null)
        {
            authenticator = new SimpleConnectionAuthenticator(userName, password);
        }
        return authenticator;
    }
}
