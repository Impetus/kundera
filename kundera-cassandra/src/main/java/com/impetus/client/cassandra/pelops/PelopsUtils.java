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

import javax.persistence.PersistenceException;

import net.dataforte.cassandra.pool.ConnectionPool;
import net.dataforte.cassandra.pool.PoolConfiguration;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.SimpleConnectionAuthenticator;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class PelopsUtils.
 */
public class PelopsUtils
{

    private static final int _DEFAULT_SHOCKET_TIMEOUT = 120000;

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
        return contactNodes + ":" + defaultPort + ":" + keyspace;
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
    public static PoolConfiguration setPoolConfigPolicy(PersistenceUnitMetadata persistenceUnitMetadata,
            PoolConfiguration prop, Map<String, Object> puProperties)
    {
        Properties props = persistenceUnitMetadata.getProperties();
        String maxActivePerNode = null;
        String maxIdlePerNode = null;
        String minIdlePerNode = null;
        String maxTotal = null;
        String testOnBorrow = null;
        String testWhileIdle = null;
        String testOnConnect = null;
        String testOnReturn = null;
        String socketTimeOut = null;
        if (puProperties != null)
        {
            maxActivePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            maxIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
            minIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
            maxTotal = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
            testOnBorrow = (String) puProperties.get(CassandraConstants.TEST_ON_BORROW);
            testOnConnect = (String) puProperties.get(CassandraConstants.TEST_ON_CONNECT);
            testOnReturn = (String) puProperties.get(CassandraConstants.TEST_ON_RETURN);
            testWhileIdle = (String) puProperties.get(CassandraConstants.TEST_WHILE_IDLE);
            socketTimeOut = (String) puProperties.get(CassandraConstants.SOCKET_TIMEOUT);
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
                prop.setInitialSize(Integer.parseInt(maxActivePerNode));
            }

            if (!StringUtils.isEmpty(maxIdlePerNode))
            {
                prop.setMaxIdle(Integer.parseInt(maxIdlePerNode));
            }

            if (!StringUtils.isEmpty(minIdlePerNode))
            {
                prop.setMinIdle(Integer.parseInt(minIdlePerNode));
            }

            if (!StringUtils.isEmpty(maxTotal))
            {
                prop.setMaxActive(Integer.parseInt(maxTotal));
            }

            CassandraSchemaMetadata csm = CassandraPropertyReader.csmd;
            Properties connProps = csm.getConnectionProperties();
            if (connProps != null)
            {
                if (testOnBorrow == null)
                {
                    testOnBorrow = connProps.getProperty(CassandraConstants.TEST_ON_BORROW);
                }
                if (testOnConnect == null)
                {
                    testOnConnect = connProps.getProperty(CassandraConstants.TEST_ON_CONNECT);
                }
                if (testOnReturn == null)
                {
                    testOnReturn = connProps.getProperty(CassandraConstants.TEST_ON_RETURN);
                }
                if (testWhileIdle == null)
                {
                    testWhileIdle = connProps.getProperty(CassandraConstants.TEST_WHILE_IDLE);
                }
                if (socketTimeOut == null)
                {
                    socketTimeOut = connProps.getProperty(CassandraConstants.SOCKET_TIMEOUT);
                }
            }

            prop.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
            prop.setTestOnConnect(Boolean.parseBoolean(testOnConnect));
            prop.setTestOnReturn(Boolean.parseBoolean(testOnReturn));
            prop.setTestWhileIdle(Boolean.parseBoolean(testWhileIdle));

            if (!StringUtils.isEmpty(socketTimeOut))
            {
                prop.setSocketTimeout(Integer.parseInt(socketTimeOut));
            }
            else
            {
                prop.setSocketTimeout(_DEFAULT_SHOCKET_TIMEOUT);
            }

        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property for " + persistenceUnitMetadata.getPersistenceUnitName()
                    + " persistence unit couldn't be parsed. Default pool policy would be used");
            prop = null;
        }
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

    /**
     * Returns instance of {@link IPooledConnection} for a given persistence
     * unit
     * 
     * @param persistenceUnit
     * @param puProperties
     * @return
     */
    public static IPooledConnection getCassandraConnection(String persistenceUnit, Map<String, Object> puProperties)
    {
        return Pelops.getDbConnPool(generatePoolName(persistenceUnit, puProperties)).getConnection();
    }

    public static void releaseConnection(IPooledConnection conn)
    {
        if (conn != null)
        {
            conn.release();
        }
    }

    public static Cassandra.Client getCassandraConnection(ConnectionPool pool)
    {
        try
        {
            if (pool != null)
            {
                return pool.getConnection();
            }
        }
        catch (TException te)
        {
            throw new PersistenceException(te);
        }
        return null;
    }

    public static void releaseConnection(ConnectionPool pool, Cassandra.Client conn)
    {
        if (pool != null && conn != null)
        {
            pool.release(conn);
        }
    }
}
