/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.cassandra.thrift;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.dataforte.cassandra.pool.ConnectionPool;
import net.dataforte.cassandra.pool.PoolConfiguration;
import net.dataforte.cassandra.pool.PoolProperties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.client.cassandra.service.CassandraHostConfigurator;
import com.impetus.client.cassandra.service.ThriftLeastActiveBalancingPolcy;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;
import com.impetus.kundera.service.policy.HostRetryService;
import com.impetus.kundera.service.policy.RoundRobinBalancingPolicy;

/**
 * A factory of {@link ThriftClient} Currently it uses Pelops for Connection
 * pooling. Need to replace it with our own pooling. Most of the code is
 * borrowed from {@link PelopsClientFactory}
 * 
 * @author amresh.singh
 */
public class ThriftClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);

    protected HostConfiguration configuration;

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new CassandraSchemaManager(ThriftClientFactory.class.getName(), externalProperty);
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
            propertyReader = new CassandraPropertyReader(externalProperties);
            propertyReader.read(getPersistenceUnit());
        }
    }

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
        ((CassandraHostRetryService) hostRetryService).shutdown();
    }

    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new CassandraEntityReader();
        initializePropertyReader();
        setExternalProperties(externalProperty);
        String loadBalancingPolicyName = CassandraPropertyReader.csmd != null ? CassandraPropertyReader.csmd
                .getConnectionProperties().getProperty(Constants.LOADBALANCING_POLICY) : null;
        initializeLoadBalancer(loadBalancingPolicyName);
        configuration = new CassandraHostConfigurator(externalProperties, CassandraPropertyReader.csmd,
                getPersistenceUnit());
        hostRetryService = new CassandraHostRetryService();
    }

    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String keyspace = null;
        if (externalProperties != null)
        {
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }

        for (CassandraHost cassandraHost : ((CassandraHostConfigurator) configuration).getCassandraHosts())
        {
            PoolConfiguration prop = new PoolProperties();
            prop.setHost(cassandraHost.getHost());
            prop.setPort(cassandraHost.getPort());
            prop.setKeySpace(keyspace);

            PelopsUtils.setPoolConfigPolicy(cassandraHost, prop);
            try
            {
                ConnectionPool pool = new ConnectionPool(prop);
                hostPools.put(cassandraHost, pool);
            }
            catch (TException e)
            {
                logger.warn("Node " + cassandraHost.getHost() + " are down");
                if (cassandraHost.isRetryHost())
                {
                    addHostToDownedHostSet(cassandraHost);
                }
            }
        }
        return null;
    }

    private void addHostToDownedHostSet(CassandraHost cassandraHost)
    {
        logger.info("Scheduling node for future retry");
        ((CassandraHostConfigurator) configuration).addHostToDownedHost(cassandraHost);
    }

    private boolean addCassandraHost(CassandraHost cassandraHost)
    {
        PoolConfiguration prop = new PoolProperties();
        prop.setHost(cassandraHost.getHost());
        prop.setPort(cassandraHost.getPort());

        PelopsUtils.setPoolConfigPolicy(cassandraHost, prop);
        try
        {
            ConnectionPool pool = new ConnectionPool(prop);
            hostPools.put(cassandraHost, pool);
            return true;
        }
        catch (TException e)
        {
            logger.warn("Node " + cassandraHost.getHost() + " are still down");
            return false;
        }
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        ConnectionPool pool = getPoolUsingPolicy();
        return new ThriftClient(this, indexManager, reader, persistenceUnit, pool, externalProperties);
    }

    /**
     * 
     * @return pool an the basis of LoadBalancing policy.
     */
    protected ConnectionPool getPoolUsingPolicy()
    {
        if (hostPools.isEmpty())
        {
            throw new KunderaException("All hosts are down. please check serevers manully.");
        }
        ConnectionPool pool = (ConnectionPool) loadBalancingPolicy.getPool(hostPools.values(), null);
        return pool;
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    class CassandraHostRetryService extends HostRetryService
    {
        private Set<CassandraHost> downedHost;

        public CassandraHostRetryService()
        {
            super(((CassandraHostConfigurator) configuration).getRetryDelay());
            sf = executor.scheduleWithFixedDelay(new RetryRunner(), this.retryDelayInSeconds, this.retryDelayInSeconds,
                    TimeUnit.SECONDS);
        }

        @Override
        protected boolean verifyConnection(Host host)
        {
            try
            {
                TSocket socket = new TSocket(host.getHost(), host.getPort());
                TTransport transport = new TFramedTransport(socket);
                TProtocol protocol = new TBinaryProtocol(transport);
                Cassandra.Client client = new Cassandra.Client(protocol);
                socket.open();
                return client.describe_cluster_name() != null;
            }
            catch (TTransportException e)
            {
                logger.warn("Node " + host.getHost() + " are still down");
                return false;
            }
            catch (TException e)
            {
                logger.warn("Node " + host.getHost() + " are still down");
                return false;
            }
        }

        /*
         * public void add(final CassandraHost cassandraHost) {
         * downedHost.add(cassandraHost);
         * 
         * // schedule a check of this host immediately, executor.submit(new
         * Runnable() {
         * 
         * @Override public void run() { if (downedHost.contains(cassandraHost)
         * && verifyConnection(cassandraHost)) { if
         * (addCassandraHost(cassandraHost)) { downedHost.remove(cassandraHost);
         * } return; } } }); }
         */

        class RetryRunner implements Runnable
        {

            @Override
            public void run()
            {
                if (!downedHost.isEmpty())
                {
                    try
                    {
                        retryDownedHosts();
                    }
                    catch (Throwable t)
                    {
                        logger.error("Error while retrying downed hosts caused by : ", t);
                    }
                }
            }

            private void retryDownedHosts()
            {

                Iterator<CassandraHost> iter = downedHost.iterator();
                while (iter.hasNext())
                {
                    CassandraHost host = iter.next();

                    if (host == null)
                    {
                        continue;
                    }

                    boolean reconnected = verifyConnection(host);
                    if (reconnected)
                    {
                        addCassandraHost(host);
                    }
                }
            }
        }

        @Override
        protected void shutdown()
        {

            if (sf != null)
            {
                sf.cancel(true);
            }
            if (executor != null)
            {
                executor.shutdownNow();
            }
        }
    }

    public ConnectionPool getOtherPoolIfAvailable(String host, int port)
    {
        CassandraHost cassandraHost = ((CassandraHostConfigurator) configuration).getCassandraHost(host, port);
        addHostToDownedHostSet(cassandraHost);
        hostPools.get(cassandraHost);
        hostPools.remove(cassandraHost);
        return getPoolUsingPolicy();
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        if (loadBalancingPolicyName != null)
        {
            switch (LoadBalancer.getValue(loadBalancingPolicyName))
            {
            case ROUNDROBIN:
                loadBalancingPolicy = new RoundRobinBalancingPolicy();
                break;
            case LEASTACTIVE:
                loadBalancingPolicy = new ThriftLeastActiveBalancingPolcy();
                break;
            }
        }
    }
}
