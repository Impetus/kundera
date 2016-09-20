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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
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
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.base.Predicate;
import com.impetus.client.cassandra.common.CassandraClientFactory;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.client.cassandra.service.CassandraHostConfiguration;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.utils.InvalidConfigurationException;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Data stax java driver client factory.
 * 
 * @author vivek.mishra
 * 
 */
public class DSClientFactory extends CassandraClientFactory
{

    /** The Constant GET_INSTANCE. */
    private static final String GET_INSTANCE = "getInstance";

    /** The Constant CUSTOM_RETRY_POLICY. */
    private static final String CUSTOM_RETRY_POLICY = "customRetryPolicy";

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(DSClientFactory.class);

    /** The configuration. */
    private CassandraHostConfiguration configuration;

    /** The keyspace. */
    private String keyspace;

    /** The session. */
    private Session session;

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
            externalProperties.put(
                    CassandraConstants.THRIFT_PORT,
                    externalProperties.get(CassandraConstants.THRIFT_PORT) != null ? externalProperties
                            .get(CassandraConstants.THRIFT_PORT) : "9160");
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
        releaseConnection(this.session);
        ((Cluster) getConnectionPoolOrConnection()).closeAsync();
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

        // initialize timestamp generator.
        initializeTimestampGenerator(externalProperty);
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
        setSessionObject(cluster); // TODO custom session
        return cluster; // TODO custom cluster
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java
     * .lang.String)
     */
    @Override
    protected Client<?> instantiateClient(String persistenceUnit)
    {
        return new DSClient(this, persistenceUnit, externalProperties, kunderaMetadata, reader, timestampGenerator);
    }

    /**
     * Sets the session object.
     * 
     * @param cluster
     *            the new session object
     */
    void setSessionObject(Cluster cluster)
    {
        this.session = cluster.connect("\"" + keyspace + "\"");
    }

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    Session getConnection()
    {
        return this.session != null ? this.session : ((Cluster) this.getConnectionPoolOrConnection()).connect("\""
                + keyspace + "\"");
    }

    /**
     * Release connection.
     * 
     * @param session
     *            the session
     */
    void releaseConnection(Session session)
    {
        if (session != null)
        {
            session.closeAsync();
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
     * Initialize property reader.
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

    /**
     * The Enum ReconnectionPolicy.
     */
    enum ReconnectionPolicy
    {

        /** The Constant reconnection policy. */
        ConstantReconnectionPolicy,
        /** The Exponential reconnection policy. */
        ExponentialReconnectionPolicy;

        /**
         * Gets the policy.
         * 
         * @param name
         *            the name
         * @return the policy
         */
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

    /**
     * The Enum BalancingPolicy.
     */
    enum BalancingPolicy
    {

        /** The DC aware round robin policy. */
        DCAwareRoundRobinPolicy,
        /** The Round robin policy. */
        RoundRobinPolicy;

        /**
         * Gets the policy.
         * 
         * @param name
         *            the name
         * @return the policy
         */
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

    /**
     * The Enum RetryPolicy.
     */
    enum RetryPolicy
    {

        /** The Downgrading consistency retry policy. */
        DowngradingConsistencyRetryPolicy,
        /** The Fallthrough retry policy. */
        FallthroughRetryPolicy,
        /** The Custom. */
        Custom;

        /**
         * Gets the policy.
         * 
         * @param name
         *            the name
         * @return the policy
         */
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

    /**
     * Gets the policy instance.
     * 
     * @param policy
     *            the policy
     * @param conProperties
     *            the con properties
     * @return the policy instance
     */
    private LoadBalancingPolicy getPolicyInstance(BalancingPolicy policy, Properties conProperties)
    {

        LoadBalancingPolicy loadBalancingPolicy = null;
        String isTokenAware = (String) conProperties.get("isTokenAware");
        String isLatencyAware = (String) conProperties.get("isLatencyAware");
        String whiteList = (String) conProperties.get("whiteList");
        String hostFilterPolicy = (String) conProperties.get("hostFilterPolicy");
        // Policy.v
        switch (policy)
        {

        case DCAwareRoundRobinPolicy:

            String usedHostsPerRemoteDc = (String) conProperties.get("usedHostsPerRemoteDc");
            String localdc = (String) conProperties.get("localdc");
            String allowRemoteDCsForLocalConsistencyLevel = (String) conProperties
                    .get("allowRemoteDCsForLocalConsistencyLevel");
            DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
            policyBuilder.withLocalDc(localdc == null ? "DC1" : localdc);
            policyBuilder.withUsedHostsPerRemoteDc(usedHostsPerRemoteDc != null ? Integer
                    .parseInt(usedHostsPerRemoteDc) : 0);
            if (allowRemoteDCsForLocalConsistencyLevel != null
                    && "true".equalsIgnoreCase(allowRemoteDCsForLocalConsistencyLevel))
            {
                policyBuilder.allowRemoteDCsForLocalConsistencyLevel();
            }
            loadBalancingPolicy = policyBuilder.build();
            break;

        // case RoundRobinPolicy:
        // loadBalancingPolicy = new RoundRobinPolicy();
        // break;

        default:
            // default is RoundRobinPolicy
            loadBalancingPolicy = new RoundRobinPolicy();
            break;
        }

        if (loadBalancingPolicy != null && Boolean.valueOf(isTokenAware))
        {
            loadBalancingPolicy = new TokenAwarePolicy(loadBalancingPolicy);
        }
        else if (loadBalancingPolicy != null && Boolean.valueOf(isLatencyAware))
        {
            loadBalancingPolicy = LatencyAwarePolicy.builder(loadBalancingPolicy).build();
        }

        if (loadBalancingPolicy != null && whiteList != null)
        {
            Collection<InetSocketAddress> whiteListCollection = buildWhiteListCollection(whiteList);

            loadBalancingPolicy = new WhiteListPolicy(loadBalancingPolicy, whiteListCollection);
        }

        if (loadBalancingPolicy != null && hostFilterPolicy != null)
        {
            Predicate<com.datastax.driver.core.Host> predicate = getHostFilterPredicate(hostFilterPolicy);

            loadBalancingPolicy = new HostFilterPolicy(loadBalancingPolicy, predicate);
        }

        return loadBalancingPolicy;
    }

    /**
     * Gets the host filter predicate.
     * 
     * @param hostFilterPolicy
     *            the host filter policy
     * @return the host filter predicate
     */
    private Predicate<com.datastax.driver.core.Host> getHostFilterPredicate(String hostFilterPolicy)
    {
        Predicate<com.datastax.driver.core.Host> predicate = null;
        Method getter = null;
        Class<?> hostFilterClazz = null;
        try
        {

            hostFilterClazz = Class.forName(hostFilterPolicy);
            getter = hostFilterClazz.getDeclaredMethod(GET_INSTANCE);

            predicate = (Predicate<com.datastax.driver.core.Host>) getter.invoke(KunderaCoreUtils
                    .createNewInstance(hostFilterClazz));
        }
        catch (ClassNotFoundException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Please make sure class " + hostFilterPolicy
                    + " set in property file exists in classpath " + e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Method " + getter.getName() + " must be declared public " + e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Please make sure getter method of " + hostFilterClazz.getSimpleName()
                    + " is named \"getInstance()\"");
        }
        catch (InvocationTargetException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Error while executing \"getInstance()\" method of Class "
                    + hostFilterClazz.getSimpleName() + ": " + e.getMessage());
        }
        catch (SecurityException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Encountered security exception while accessing the method: "
                    + "\"getInstance()\"" + e.getMessage());
        }
        return predicate;
    }

    /**
     * Builds the white list collection.
     * 
     * @param whiteList
     *            the white list
     * @return the collection
     */
    private Collection<InetSocketAddress> buildWhiteListCollection(String whiteList)
    {
        String[] list = whiteList.split(Constants.COMMA);
        Collection<InetSocketAddress> whiteListCollection = new ArrayList<InetSocketAddress>();

        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());
        Properties props = persistenceUnitMetadata.getProperties();
        int defaultPort = 9042;

        if (externalProperties != null && externalProperties.get(PersistenceProperties.KUNDERA_PORT) != null)
        {
            try
            {
                defaultPort = Integer.parseInt((String) externalProperties.get(PersistenceProperties.KUNDERA_PORT));
            }
            catch (NumberFormatException e)
            {
                logger.error("Port in persistence.xml should be integer");
            }
        }

        else
        {
            try
            {
                defaultPort = Integer.parseInt((String) props.get(PersistenceProperties.KUNDERA_PORT));
            }
            catch (NumberFormatException e)
            {
                logger.error("Port in persistence.xml should be integer");
            }
        }

        for (String node : list)
        {
            if (node.indexOf(Constants.COLON) > 0)
            {
                String[] parts = node.split(Constants.COLON);
                whiteListCollection.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            }
            else
            {
                whiteListCollection.add(new InetSocketAddress(node, defaultPort));
            }
        }
        return whiteListCollection;
    }

    /**
     * Gets the policy.
     * 
     * @param policy
     *            the policy
     * @param props
     *            the props
     * @return the policy
     */
    private com.datastax.driver.core.policies.ReconnectionPolicy getPolicy(ReconnectionPolicy policy, Properties props)
    {
        com.datastax.driver.core.policies.ReconnectionPolicy reconnectionPolicy = null;
        switch (policy)
        {
        case ConstantReconnectionPolicy:
            String property = props.getProperty("constantDelayMs");
            long constantDelayMs = property != null ? new Long(property) : 0l;
            reconnectionPolicy = new ConstantReconnectionPolicy(constantDelayMs);
            break;

        case ExponentialReconnectionPolicy:
            String baseDelayMsAsStr = props.getProperty("baseDelayMs");
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

    /**
     * Gets the policy.
     * 
     * @param policy
     *            the policy
     * @param props
     *            the props
     * @return the policy
     * @throws Exception
     *             the exception
     */
    private com.datastax.driver.core.policies.RetryPolicy getPolicy(RetryPolicy policy, Properties props)
    {
        com.datastax.driver.core.policies.RetryPolicy retryPolicy = null;

        String isLoggingRetry = (String) props.get("isLoggingRetry");

        switch (policy)
        {
        case DowngradingConsistencyRetryPolicy:
            retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
            break;

        case FallthroughRetryPolicy:
            retryPolicy = FallthroughRetryPolicy.INSTANCE;
            break;

        case Custom:
            retryPolicy = getCustomRetryPolicy(props);
            break;

        default:
            break;
        }

        if (retryPolicy != null && Boolean.valueOf(isLoggingRetry))
        {
            retryPolicy = new LoggingRetryPolicy(retryPolicy);
        }

        return retryPolicy;

    }

    /**
     * Gets the custom retry policy.
     * 
     * @param props
     *            the props
     * @return the custom retry policy
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws Exception
     *             the exception
     */
    private com.datastax.driver.core.policies.RetryPolicy getCustomRetryPolicy(Properties props)

    {
        String customRetryPolicy = (String) props.get(CUSTOM_RETRY_POLICY);
        Class<?> clazz = null;
        Method getter = null;
        try
        {
            clazz = Class.forName(customRetryPolicy);
            com.datastax.driver.core.policies.RetryPolicy retryPolicy = (com.datastax.driver.core.policies.RetryPolicy) KunderaCoreUtils
                    .createNewInstance(clazz);
            if (retryPolicy != null)
            {
                return retryPolicy;
            }
            getter = clazz.getDeclaredMethod(GET_INSTANCE);
            return (com.datastax.driver.core.policies.RetryPolicy) getter.invoke(null, (Object[]) null);
        }
        catch (ClassNotFoundException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Please make sure class " + customRetryPolicy
                    + " set in property file exists in classpath " + e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Method " + getter.getName() + " must be declared public " + e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Please make sure getter method of " + clazz.getSimpleName()
                    + " is named \"getInstance()\"");
        }
        catch (InvocationTargetException e)
        {
            logger.error(e.getMessage());
            throw new KunderaException("Error while executing \"getInstance()\" method of Class "
                    + clazz.getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Gets the socket options.
     * 
     * @param connectionProperties
     *            the connection properties
     * @return the socket options
     */
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

    /**
     * Gets the pooling options.
     * 
     * @param connectionProperties
     *            the connection properties
     * @return the pooling options
     */
    private PoolingOptions getPoolingOptions(Properties connectionProperties)
    {
        // minSimultaneousRequests, maxSimultaneousRequests, coreConnections,
        // maxConnections

        PoolingOptions options = new PoolingOptions();

        String hostDistance = connectionProperties.getProperty("hostDistance");
        String maxConnectionsPerHost = connectionProperties.getProperty("maxConnectionsPerHost");
        String maxRequestsPerConnection = connectionProperties.getProperty("maxRequestsPerConnection");
        String coreConnections = connectionProperties.getProperty("coreConnections");

        if (!StringUtils.isBlank(hostDistance))
        {
            HostDistance hostDist = HostDistance.valueOf(hostDistance.toUpperCase());
            if (!StringUtils.isBlank(coreConnections))
            {
                options.setCoreConnectionsPerHost(HostDistance.LOCAL, new Integer(coreConnections));
            }

            if (!StringUtils.isBlank(maxConnectionsPerHost))
            {
                options.setMaxConnectionsPerHost(hostDist, new Integer(maxConnectionsPerHost));
            }

            if (!StringUtils.isBlank(maxRequestsPerConnection))
            {
                options.setMaxRequestsPerConnection(hostDist, new Integer(maxRequestsPerConnection));
            }
        }
        return options;
    }

}
