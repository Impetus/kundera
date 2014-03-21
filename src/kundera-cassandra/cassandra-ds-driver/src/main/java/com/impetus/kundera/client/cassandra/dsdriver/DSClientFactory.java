/**
 * Copyright 2014 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.client.cassandra.dsdriver;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.impetus.client.cassandra.common.CassandraClientFactory;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.client.cassandra.service.CassandraHostConfiguration;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * Data stax java driver client factory.
 * 
 * @author vivek.mishra
 * 
 */
public class DSClientFactory extends GenericClientFactory implements CassandraClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(DSClientFactory.class);

    private CassandraHostConfiguration configuration;

    private String keyspace;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            externalProperties.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
            schemaManager = new CassandraSchemaManager(this.getClass().getName(), externalProperties, kunderaMetadata);
        }
        return schemaManager;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        if (indexManager != null)
        {
            indexManager.close();
        }
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
        schemaManager = null;
        externalProperties = null;

        ((Cluster) getConnectionPoolOrConnection()).shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.cassandra.common.CassandraClientFactory#addCassandraHost
     * (com.impetus.client.cassandra.service.CassandraHost)
     */
    @Override
    public boolean addCassandraHost(CassandraHost host)
    {
        // TODO Auto-generated method stub
        // No need to setup this, as it is part of Kundera powered retry
        // service.

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new CassandraEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(externalProperty);

        configuration = new CassandraHostConfiguration(externalProperties, CassandraPropertyReader.csmd,
                getPersistenceUnit(), kunderaMetadata);

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
        // AuthInfoProvider,LoadBalancingPolicy, ReconnectionPolicy,
        // RetryPolicy,ProtocolOptions.Compression, SSLOptions,
        // PoolingOptions,SocketOptions

        if (logger.isDebugEnabled())
        {
            logger.debug("Intiatilzing connection");
        }

        Properties connectionProperties = CassandraPropertyReader.csmd.getConnectionProperties();
        Builder connectionBuilder = Cluster.builder();

        // add host/port and AuthInfoProvider
        for (Host host : configuration.getCassandraHosts())
        {
            connectionBuilder.addContactPoint(host.getHost()).withPort(host.getPort());
            if (host.getUser() != null)
            {
                connectionBuilder.withCredentials(host.getUser(), host.getPassword());
            }
        }

        // add policy configuration
        String loadBalancingPolicy = connectionProperties.getProperty(Constants.LOADBALANCING_POLICY);
        if (!StringUtils.isBlank(loadBalancingPolicy))
        {
            LoadBalancingPolicy policy = getPolicyInstance(BalancingPolicy.getPolicy(loadBalancingPolicy),
                    connectionProperties);
            if (policy != null)
            {
                connectionBuilder.withLoadBalancingPolicy(policy);
            }
        }

        // compression
        String compression = connectionProperties.getProperty("compression");

        if (!StringUtils.isBlank(compression))
        {
            connectionBuilder.withCompression(Compression.valueOf(compression));
        }

        // ReconnectionPolicy

        String reconnectionPolicy = connectionProperties.getProperty("reconnection.policy");
        if (!StringUtils.isBlank(reconnectionPolicy))
        {
            com.datastax.driver.core.policies.ReconnectionPolicy policy = getPolicy(
                    ReconnectionPolicy.getPolicy(reconnectionPolicy), connectionProperties);

            if (policy != null)
            {
                connectionBuilder.withReconnectionPolicy(policy);
            }

        }

        // , RetryPolicy
        String retryPolicy = connectionProperties.getProperty("retry.policy");

        if (!StringUtils.isBlank(retryPolicy))
        {
            com.datastax.driver.core.policies.RetryPolicy policy = getPolicy(RetryPolicy.getPolicy(retryPolicy),
                    connectionProperties);

            if (policy != null)
            {
                connectionBuilder.withRetryPolicy(policy);
            }

        }

        // TODO::: SSLOptions? Not sure how to add it.

        // SocketOptions
        connectionBuilder.withSocketOptions(getSocketOptions(connectionProperties));

        // PoolingOptions,
        connectionBuilder.withPoolingOptions(getPoolingOptions(connectionProperties));

        // finally build cluster.
        Cluster cluster = connectionBuilder.build();

        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());
        Properties props = persistenceUnitMetadata.getProperties();

        if (externalProperties != null)
        {
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        // Session session = cluster.connect("\""+keyspace+"\"");

        // cluster.s
        return cluster;
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
        return new DSClient(this, persistenceUnit, externalProperties, kunderaMetadata, reader);
    }

    Session getConnection()
    {
        Session session = ((Cluster) this.getConnectionPoolOrConnection()).connect("\"" + keyspace + "\"");
        return session;
    }

    void releaseConnection(Session session)
    {
        if (session != null)
        {
            session.shutdown();
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
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer
     * (java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Method not supported for datastax java driver");
    }

    /**
     * 
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new CassandraPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    enum ReconnectionPolicy
    {
        ConstantReonnectionPolicy, ExponentialReconnectionPolicy;

        static ReconnectionPolicy getPolicy(String name)
        {
            for (ReconnectionPolicy p : ReconnectionPolicy.values())
            {
                if (p.name().equalsIgnoreCase(name))
                {
                    return p;
                }
            }

            logger.error("Invalid policy name {} provided, supported policies are {}", name,
                    ReconnectionPolicy.values());

            throw new InvalidConfigurationException("Invalid policy name " + name + " provided!");
        }
    }

    enum BalancingPolicy
    {
        DCAwareRoundRobinPolicy, RoundRobinPolicy;

        static BalancingPolicy getPolicy(String name)
        {
            for (BalancingPolicy p : BalancingPolicy.values())
            {
                if (p.name().equalsIgnoreCase(name))
                {
                    return p;
                }
            }

            logger.error("Invalid policy name {} provided, supported policies are {}", name, BalancingPolicy.values());

            throw new InvalidConfigurationException("Invalid policy name " + name + " provided!");
        }

    }

    enum RetryPolicy
    {
        DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy;

        static RetryPolicy getPolicy(String name)
        {
            for (RetryPolicy p : RetryPolicy.values())
            {
                if (p.name().equalsIgnoreCase(name))
                {
                    return p;
                }
            }

            logger.error("Invalid policy name {} provided, supported policies are {}", name, RetryPolicy.values());

            throw new InvalidConfigurationException("Invalid policy name " + name + " provided!");
        }
    }

    private LoadBalancingPolicy getPolicyInstance(BalancingPolicy policy, Properties conProperties)
    {

        LoadBalancingPolicy loadBalancingPolicy = null;
        String isTokenAware = (String) conProperties.get("isTokenAware");
        // Policy.v
        switch (policy)
        {

        case DCAwareRoundRobinPolicy:

            String usedHostsPerRemoteDc = (String) conProperties.get("usedHostsPerRemoteDc");
            loadBalancingPolicy = new DCAwareRoundRobinPolicy((String) conProperties.get("localdc"),
                    usedHostsPerRemoteDc != null ? Integer.parseInt(usedHostsPerRemoteDc):0);
            break;

        case RoundRobinPolicy:
            loadBalancingPolicy = new RoundRobinPolicy();
            break;

        default:
            // default is LatencyAwarePolicy
            break;
        }

        if (loadBalancingPolicy != null && StringUtils.isBlank(isTokenAware) && Boolean.valueOf(isTokenAware))
        {
            loadBalancingPolicy = new TokenAwarePolicy(loadBalancingPolicy);
        }

        return loadBalancingPolicy;
    }

    private com.datastax.driver.core.policies.ReconnectionPolicy getPolicy(ReconnectionPolicy policy, Properties props)
    {
        com.datastax.driver.core.policies.ReconnectionPolicy reconnectionPolicy = null;
        switch (policy)
        {
        case ConstantReonnectionPolicy:
            String property = props.getProperty("constantDelayMs");
            long constantDelayMs = property != null ? new Long(property): 0l;
            reconnectionPolicy = new ConstantReconnectionPolicy(constantDelayMs);
            break;

        case ExponentialReconnectionPolicy:
            String baseDelayMsAsStr= props.getProperty("baseDelayMs") ;
            String maxDelayMsAsStr = props.getProperty("maxDelayMs"); 
            if (!StringUtils.isBlank(baseDelayMsAsStr) && !StringUtils.isBlank(maxDelayMsAsStr))
            {
                long baseDelayMs = new Long(baseDelayMsAsStr);
                long maxDelayMs = new Long(maxDelayMsAsStr);
                reconnectionPolicy = new ExponentialReconnectionPolicy(baseDelayMs, maxDelayMs);
            }
            break;

        default:
            break;
        }

        return reconnectionPolicy;
    }

    private com.datastax.driver.core.policies.RetryPolicy getPolicy(RetryPolicy policy, Properties props)
    {
        com.datastax.driver.core.policies.RetryPolicy retryPolicy = null;

        String isTokenAware = (String) props.get("isLoggingRetry");
        switch (policy)
        {
        case DowngradingConsistencyRetryPolicy:
            retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
            break;

        case FallthroughRetryPolicy:
            retryPolicy = FallthroughRetryPolicy.INSTANCE;
            break;

        default:
            break;
        }

        if (retryPolicy != null && StringUtils.isBlank(isTokenAware) && Boolean.valueOf(isTokenAware))
        {
            retryPolicy = new LoggingRetryPolicy(retryPolicy);
        }

        return retryPolicy;

    }

    private SocketOptions getSocketOptions(Properties connectionProperties)
    {
        // SocketOptions
        SocketOptions socketConfig = new SocketOptions();

        String connectTimeoutMillis = connectionProperties.getProperty(CassandraConstants.SOCKET_TIMEOUT);
        String readTimeoutMillis = connectionProperties.getProperty("readTimeoutMillis");
        String keepAlive = connectionProperties.getProperty("keepAlive");
        String reuseAddress = connectionProperties.getProperty("reuseAddress");
        String soLinger = connectionProperties.getProperty("soLinger");
        String tcpNoDelay = connectionProperties.getProperty("tcpNoDelay");
        String receiveBufferSize = connectionProperties.getProperty("receiveBufferSize");
        String sendBufferSize = connectionProperties.getProperty("sendBufferSize");

        if (!StringUtils.isBlank(connectTimeoutMillis))
        {
            socketConfig.setConnectTimeoutMillis(new Integer(connectTimeoutMillis));
        }

        if (!StringUtils.isBlank(readTimeoutMillis))
        {
            socketConfig.setReadTimeoutMillis(new Integer(readTimeoutMillis));
        }

        if (!StringUtils.isBlank(keepAlive))
        {
            socketConfig.setKeepAlive(new Boolean(keepAlive));
        }

        if (!StringUtils.isBlank(reuseAddress))
        {
            socketConfig.setReuseAddress(new Boolean(reuseAddress));
        }

        if (!StringUtils.isBlank(soLinger))
        {
            socketConfig.setSoLinger(new Integer(soLinger));
        }

        if (!StringUtils.isBlank(tcpNoDelay))
        {
            socketConfig.setTcpNoDelay(new Boolean(tcpNoDelay));
        }

        if (!StringUtils.isBlank(receiveBufferSize))
        {
            socketConfig.setReceiveBufferSize(new Integer(receiveBufferSize));
        }

        if (!StringUtils.isBlank(sendBufferSize))
        {
            socketConfig.setSendBufferSize(new Integer(sendBufferSize));
        }

        return socketConfig;
    }

    private PoolingOptions getPoolingOptions(Properties connectionProperties)
    {
        // minSimultaneousRequests, maxSimultaneousRequests, coreConnections,
        // maxConnections

        PoolingOptions options = new PoolingOptions();

        String hostDistance = connectionProperties.getProperty("hostDistance");
        String minSimultaneousRequests = connectionProperties.getProperty("minSimultaneousRequests");
        String maxSimultaneousRequests = connectionProperties.getProperty("maxSimultaneousRequests");
        String coreConnections = connectionProperties.getProperty("coreConnections");
        String maxConnections = connectionProperties.getProperty("maxConnections");

        if (!StringUtils.isBlank(hostDistance))
        {
            HostDistance hostDist = HostDistance.valueOf(hostDistance.toUpperCase());
            if (!StringUtils.isBlank(coreConnections))
            {
                options.setCoreConnectionsPerHost(HostDistance.LOCAL, new Integer(coreConnections));
            }

            if (!StringUtils.isBlank(maxConnections))
            {
                options.setMaxConnectionsPerHost(hostDist, new Integer(maxConnections));
            }

            if (!StringUtils.isBlank(minSimultaneousRequests))
            {
                options.setMinSimultaneousRequestsPerConnectionThreshold(hostDist, new Integer(minSimultaneousRequests));
            }

            if (!StringUtils.isBlank(maxSimultaneousRequests))
            {
                options.setMaxSimultaneousRequestsPerConnectionThreshold(hostDist, new Integer(maxSimultaneousRequests));
            }
        }

        return options;

    }

}
