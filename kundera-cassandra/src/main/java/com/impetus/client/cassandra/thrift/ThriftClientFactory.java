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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.dataforte.cassandra.pool.ConnectionPool;
import net.dataforte.cassandra.pool.PoolConfiguration;
import net.dataforte.cassandra.pool.PoolProperties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.client.cassandra.schemamanager.CassandraSchemaManager;
import com.impetus.client.cassandra.service.CassandraHost;
import com.impetus.client.cassandra.service.CassandraHostConfiguration;
import com.impetus.client.cassandra.service.CassandraRetryService;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;
import com.impetus.kundera.service.policy.LeastActiveBalancingPolicy;
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
//            setExternalProperties(externalProperty);
            schemaManager = new CassandraSchemaManager(ThriftClientFactory.class.getName(), externalProperties);
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

        for (Object connectionPool : hostPools.values())
        {
            if (connectionPool != null && connectionPool.getClass().isAssignableFrom(ConnectionPool.class))
            {
                ((ConnectionPool) connectionPool).close(true);
            }
        }
        ((CassandraRetryService) hostRetryService).shutdown();
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
        configuration = new CassandraHostConfiguration(externalProperties, CassandraPropertyReader.csmd,
                getPersistenceUnit());
        hostRetryService = new CassandraRetryService(configuration, this);
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

        for (Host host : ((CassandraHostConfiguration) configuration).getCassandraHosts())
        {
            PoolConfiguration prop = new PoolProperties();
            prop.setHost(host.getHost());
            prop.setPort(host.getPort());
            prop.setKeySpace(keyspace);

            PelopsUtils.setPoolConfigPolicy((CassandraHost) host, prop);
            try
            {
                ConnectionPool pool = new ConnectionPool(prop);
                hostPools.put(host, pool);
            }
            catch (TException e)
            {
                logger.warn("Node {} are down, Caused by {} .", host.getHost(), e.getMessage());
                if (host.isRetryHost())
                {
                    logger.warn("Scheduling node for future retry");
                    ((CassandraRetryService) hostRetryService).add((CassandraHost) host);
                }
            }
        }
        return null;
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
    private ConnectionPool getPoolUsingPolicy()
    {
        if (!hostPools.isEmpty())
        {
            return (ConnectionPool) loadBalancingPolicy.getPool(hostPools.values());
        }
        throw new KunderaException("All hosts are down. please check servers manully.");
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    /**
     * 
     * @param host
     * @param port
     * @return
     */
    private ConnectionPool getNewPool(String host, int port)
    {
        CassandraHost cassandraHost = ((CassandraHostConfiguration) configuration).getCassandraHost(host, port);
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
            default:
                loadBalancingPolicy = new RoundRobinBalancingPolicy();
                break;
            }
        }
        loadBalancingPolicy = new RoundRobinBalancingPolicy();
    }

    Connection getConnection(ConnectionPool pool)
    {
        ConnectionPool connectionPool = pool;
        boolean success = false;
        while (!success)
        {
            try
            {
                success = true;
                Cassandra.Client client = connectionPool.getConnection();
                if(logger.isInfoEnabled())
                {
                    logger.info("Retruning connection of {} :{} .", pool.getPoolProperties().getHost(), pool
                        .getPoolProperties().getPort());
                }
                
                return new Connection(client, connectionPool);
            }
            catch (TException te)
            {
                success = false;
                logger.warn("{} :{}  host appears to be down, trying for next ", pool.getPoolProperties().getHost(),
                        pool.getPoolProperties().getPort());
                connectionPool = getNewPool(pool.getPoolProperties().getHost(), pool.getPoolProperties().getPort());
            }
        }
        
      throw new KunderaException("All hosts are down. please check servers manully.");
    }

    void releaseConnection(ConnectionPool pool, Cassandra.Client conn)
    {
        if (pool != null && conn != null)
        {
            pool.release(conn);
        }
    }

    /**
     * Adds a pool in hostPools map for given host.
     * 
     * @param cassandraHost
     * @return true id added successfully.
     */
    public boolean addCassandraHost(CassandraHost cassandraHost)
    {
        String keysapce = KunderaMetadataManager.getPersistenceUnitMetadata(getPersistenceUnit()).getProperties()
                .getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
        PoolConfiguration prop = new PoolProperties();
        prop.setHost(cassandraHost.getHost());
        prop.setPort(cassandraHost.getPort());
        prop.setKeySpace(keysapce);

        PelopsUtils.setPoolConfigPolicy(cassandraHost, prop);
        try
        {
            if (logger.isInfoEnabled())
            {
                logger.info("Initializing connection for keyspace {},host {},port {}", keysapce,
                        cassandraHost.getHost(), cassandraHost.getPort());
            }
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

    /**
     * Extends LeastActiveBalancingPolicy class and provide own implementation
     * in order to support least active balancing policy.
     * 
     * @author Kuldeep.Mishra
     * 
     */
    private class ThriftLeastActiveBalancingPolcy extends LeastActiveBalancingPolicy
    {

        /**
         * 
         * @return pool object for host which has least active connections
         *         determined by maxActive connection.
         * 
         */
        @Override
        public Object getPool(Collection<Object> pools)
        {
            List<Object> vals = Lists.newArrayList(pools);
            Collections.shuffle(vals);
            Collections.sort(vals, new ShufflingCompare());
            Collections.reverse(vals);
            Iterator<Object> iterator = vals.iterator();
            Object concurrentConnectionPool = iterator.next();
            return concurrentConnectionPool;
        }

        /**
         * Compares two pool object on the basis of their maxActive connection.
         * 
         * @author Kuldeep Mishra
         * 
         */
        private final class ShufflingCompare implements Comparator<Object>
        {
            public int compare(Object o1, Object o2)
            {
                PoolConfiguration props1 = ((ConnectionPool) o1).getPoolProperties();
                PoolConfiguration props2 = ((ConnectionPool) o2).getPoolProperties();

                int activeConnections1 = ((ConnectionPool) o1).getActive();
                int activeConnections2 = ((ConnectionPool) o2).getActive();

                return (props1.getMaxActive() - activeConnections1) - (props2.getMaxActive() - activeConnections2);
            }
        }
    }

    /**
     * Connection class holds client and related pool.
     * 
     * @author Kuldeep.Mishra
     * 
     */
    public class Connection
    {
        private Cassandra.Client client;

        private ConnectionPool pool;

        public Connection(org.apache.cassandra.thrift.Cassandra.Client client, ConnectionPool pool)
        {
            this.client = client;
            this.pool = pool;
        }

        public Cassandra.Client getClient()
        {
            return client;
        }

        public ConnectionPool getPool()
        {
            return pool;
        }
    }
}
