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
package com.impetus.client.cassandra.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import net.dataforte.cassandra.pool.HostFailoverPolicy;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;

/**
 * Holds host configuration for cassandra specific settings.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class CassandraHostConfiguration extends HostConfiguration
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CassandraHostConfiguration.class);

    public CassandraHostConfiguration(Map externalProperties, CassandraSchemaMetadata csmd, String persistenceUnit,
            final KunderaMetadata kunderaMetadata)
    {
        super(externalProperties, csmd != null ? csmd.getConnectionServers() : new ArrayList<Server>(),
                persistenceUnit, kunderaMetadata);
        String property = csmd.getConnectionProperties().getProperty(Constants.RETRY_DELAY);
        if (StringUtils.isNumeric(property))
        {
            retryDelay = Integer.parseInt(property);
        }
    }

    protected void buildHosts(List<Server> servers, List<Host> hostsList)
    {
        List<CassandraHost> cassandraHosts = new CopyOnWriteArrayList<CassandraHost>();
        for (Server server : servers)
        {
            String host = server.getHost().trim();
            String portAsString = server.getPort().trim();
            onValidation(host, portAsString);
            Properties serverProperties = server.getProperties();
            CassandraHost cassandraHost = new CassandraHost(host, Integer.parseInt(portAsString));
            setConfig(cassandraHost, null, serverProperties);
            cassandraHosts.add(cassandraHost);
            hostsList.add(cassandraHost);
        }
    }

    protected void buildHosts(String hosts, String portAsString, List<Host> hostsList)
    {
        String[] hostVals = hosts.split(",");
        List<CassandraHost> cassandraHosts = new CopyOnWriteArrayList<CassandraHost>();
        for (int x = 0; x < hostVals.length; x++)
        {
            String host = hostVals[x].trim();
            portAsString = portAsString.trim();
            onValidation(host, portAsString);
            int port = Integer.parseInt(portAsString);
            CassandraHost cassandraHost = port == CassandraHost.DEFAULT_PORT ? new CassandraHost(host)
                    : new CassandraHost(host, port);
            setConfig(cassandraHost, persistenceUnitMetadata.getProperties(), externalProperties);
            cassandraHosts.add(cassandraHost);
            hostsList.add(cassandraHost);
        }
    }

    @Override
    protected void setConfig(Host host, Properties props, Map externalProperties)
    {
        CassandraHost cassandraHost = (CassandraHost) host;
        String maxActivePerNode = null;
        String maxIdlePerNode = null;
        String minIdlePerNode = null;
        String maxTotal = null;
        String testOnBorrow = null;
        String testWhileIdle = null;
        String testOnConnect = null;
        String testOnReturn = null;
        String socketTimeOut = null;
        String userName = null;
        String password = null;
        String maxWaitInMilli = null;
        String failOverPolicy = null;
        boolean hostRetry = false;
        if (externalProperties != null)
        {
            maxActivePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            maxIdlePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
            minIdlePerNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
            maxTotal = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
            testOnBorrow = (String) externalProperties.get(CassandraConstants.TEST_ON_BORROW);
            testOnConnect = (String) externalProperties.get(CassandraConstants.TEST_ON_CONNECT);
            testOnReturn = (String) externalProperties.get(CassandraConstants.TEST_ON_RETURN);
            testWhileIdle = (String) externalProperties.get(CassandraConstants.TEST_WHILE_IDLE);
            socketTimeOut = (String) externalProperties.get(CassandraConstants.SOCKET_TIMEOUT);
            userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
            maxWaitInMilli = (String) externalProperties.get(CassandraConstants.MAX_WAIT);
        }

        if (props != null)
        {
            if (maxActivePerNode == null)
            {
                maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE).trim() : null;
            }
            if (maxIdlePerNode == null)
            {
                maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE).trim() : null;
            }
            if (minIdlePerNode == null)
            {
                minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE).trim() : null;
            }
            if (maxTotal == null)
            {
                maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL) != null ? props
                        .getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL).trim() : null;
            }
            if (maxWaitInMilli == null)
            {
                maxWaitInMilli = props.getProperty(CassandraConstants.MAX_WAIT) != null ? props.getProperty(
                        CassandraConstants.MAX_WAIT).trim() : null;
            }

            if (userName == null)
            {
                userName = props.getProperty(PersistenceProperties.KUNDERA_USERNAME);
                password = props.getProperty(PersistenceProperties.KUNDERA_PASSWORD);
            }
        }
        try
        {
            if (!StringUtils.isEmpty(maxActivePerNode))
            {
                cassandraHost.setInitialSize(Integer.parseInt(maxActivePerNode));
                cassandraHost.setMaxActive(Integer.parseInt(maxActivePerNode));
            }

            if (!StringUtils.isEmpty(maxIdlePerNode))
            {
                cassandraHost.setMaxIdle(Integer.parseInt(maxIdlePerNode));
            }

            if (!StringUtils.isEmpty(minIdlePerNode))
            {
                cassandraHost.setMinIdle(Integer.parseInt(minIdlePerNode));
            }

            if (!StringUtils.isEmpty(maxTotal))
            {
                cassandraHost.setMaxActive(Integer.parseInt(maxTotal));
            }
            if (!StringUtils.isEmpty(maxWaitInMilli))
            {
                cassandraHost.setMaxWait(Integer.parseInt(maxWaitInMilli));
            }

            cassandraHost.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
            cassandraHost.setTestOnConnect(Boolean.parseBoolean(testOnConnect));
            cassandraHost.setTestOnReturn(Boolean.parseBoolean(testOnReturn));
            cassandraHost.setTestWhileIdle(Boolean.parseBoolean(testWhileIdle));
            cassandraHost.setHostFailoverPolicy(getFailoverPolicy(failOverPolicy));
            cassandraHost.setRetryHost(hostRetry);
            cassandraHost.setUserName(userName);
            cassandraHost.setPassword(password);

            if (!StringUtils.isEmpty(socketTimeOut))
            {
                cassandraHost.setSocketTimeout(Integer.parseInt(socketTimeOut));
            }
            else
            {
                cassandraHost.setSocketTimeout(CassandraHost.DEFAULT_SHOCKET_TIMEOUT);
            }
        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property couldn't be parsed. Default pool policy would be used");
        }
    }

    /**
     * 
     * @return Host array
     */
    public List<Host> getCassandraHosts()
    {
        return hostsList;
    }

    /**
     * Resolve failover policy for Cassandra thrift.
     * 
     * @param failoverOption
     * @return
     */
    private HostFailoverPolicy getFailoverPolicy(String failoverOption)
    {
        if (failoverOption != null)
        {
            if (Constants.FAIL_FAST.equals(failoverOption))
            {
                return HostFailoverPolicy.FAIL_FAST;
            }
            else if (Constants.ON_FAIL_TRY_ALL_AVAILABLE.equals(failoverOption))
            {
                return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
            }
            else if (Constants.ON_FAIL_TRY_ONE_NEXT_AVAILABLE.equals(failoverOption))
            {
                return HostFailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE;
            }
            else
            {
                logger.warn("Invalid failover policy {}, using default {} ", failoverOption,
                        HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE.name());
                return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
            }
        }
        return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    }

    /**
     * 
     * @return
     */
    public int getRetryDelay()
    {
        return retryDelay;
    }

    /**
     * 
     * @param host
     * @param port
     * @return CassandraHosts
     */
    public CassandraHost getCassandraHost(String host, int port)
    {
        for (Host cassandraHost : hostsList)
        {
            if (((CassandraHost) cassandraHost).equals(new CassandraHost(host, port)))
            {
                return (CassandraHost) cassandraHost;
            }
        }
        return null;
    }
}