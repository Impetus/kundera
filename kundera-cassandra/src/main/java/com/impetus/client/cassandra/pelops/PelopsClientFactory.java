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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Cluster.Node;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.TransportException;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
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
import com.impetus.kundera.service.policy.LoadBalancingPolicy;
import com.impetus.kundera.service.policy.RoundRobinBalancingPolicy;

/**
 * A factory for creating PelopsCliobjects.
 */
public class PelopsClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(PelopsClientFactory.class);

    private HostConfiguration configuration;

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
        logger.info("Creating pool");
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
            CassandraHost cassandraHost = (CassandraHost) host;
            String poolName = PelopsUtils.generatePoolName(cassandraHost.getHost(), cassandraHost.getPort(), keyspace);

            if (Pelops.getDbConnPool(poolName) == null)
            {
                try
                {
                    Cluster cluster = new Cluster(cassandraHost.getHost(), new IConnection.Config(
                            cassandraHost.getPort(), true, -1, PelopsUtils.getAuthenticationRequest(
                                    cassandraHost.getUser(), cassandraHost.getPassword())), false);

                    if (logger.isInfoEnabled())
                    {
                        logger.info("Initializing connection pool for keyspace {}, host {},port {}.", keyspace,
                                cassandraHost.getHost(), cassandraHost.getPort());
                    }

                    Policy policy = PelopsUtils.getPoolConfigPolicy(cassandraHost);

                    // Add pool with specified policy. null means default
                    // operand
                    // policy.
                    Pelops.addPool(poolName, cluster, keyspace, policy, null);
                }
                catch (TransportException e)
                {
                    logger.warn("Node " + host.getHost() + " are down");
                    if (host.isRetryHost())
                    {
                        logger.info("Scheduling node for future retry");
                        ((CassandraRetryService) hostRetryService).add((CassandraHost) host);
                    }
                }
            }
            hostPools.put(cassandraHost, Pelops.getDbConnPool(poolName));
        }
        // TODO return a thrift pool
        return null;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        if (logger.isInfoEnabled())
        {
            logger.info("Initializing pelops client for persistence unit {}", persistenceUnit);
        }
        IThriftPool pool = getPoolUsingPolicy();
        return new PelopsClient(indexManager, reader, this, persistenceUnit, externalProperties, pool);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
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
        // Pelops.shutdown();
        // Pelops.removePool(PelopsUtils.generatePoolName(getPersistenceUnit(),
        // externalProperties));
        externalProperties = null;
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> externalProperty)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(externalProperty);
            schemaManager = new CassandraSchemaManager(PelopsClientFactory.class.getName(), externalProperty);
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
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        switch (LoadBalancer.getValue(loadBalancingPolicyName))
        {
        case ROUNDROBIN:
            loadBalancingPolicy = new RoundRobinBalancingPolicy();
            break;
        case LEASTACTIVE:
            loadBalancingPolicy = new PelopsLeastActiveBalancingPolcy();
            break;
        default:
            loadBalancingPolicy = new RoundRobinBalancingPolicy();
            break;
        }
    }

    /**
     * 
     * @param host
     * @param port
     * @return
     */
    private IThriftPool getNewPool(String host, int port)
    {
        CassandraHost cassandraHost = ((CassandraHostConfiguration) configuration).getCassandraHost(host, port);
        hostPools.remove(cassandraHost);
        return getPoolUsingPolicy();
    }

    /**
     * 
     * @return pool an the basis of LoadBalancing policy.
     */
    private IThriftPool getPoolUsingPolicy()
    {
        IThriftPool pool = null;
        if (!hostPools.isEmpty())
        {
            pool = (IThriftPool) loadBalancingPolicy.getPool(hostPools.values());
        }
        return pool;
    }

    IPooledConnection getConnection(IThriftPool pool)
    {
        boolean success = false;
        while (!success)
        {
            try
            {
                success = true;
                if (pool != null)
                {
                    return pool.getConnection();
                }
            }
            catch (TransportException te)
            {
                success = false;
                Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
                logger.warn("{} :{}  host appears to be down, trying for next ", nodes, ((CommonsBackedPool) pool)
                        .getCluster().getConnectionConfig().getThriftPort());
                pool = getNewPool(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster().getConnectionConfig()
                        .getThriftPort());
            }
        }
        throw new KunderaException("All hosts are down. please check servers manully.");
    }

    Mutator getMutator(IThriftPool pool)
    {
        boolean success = false;
        while (!success)
        {
            try
            {
                success = true;
                if (pool != null)
                {
                    return Pelops.createMutator(getPoolName(pool));
                }
            }
            catch (TransportException te)
            {
                success = false;
                Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
                logger.warn("{} :{}  host appears to be down, trying for next ", nodes, ((CommonsBackedPool) pool)
                        .getCluster().getConnectionConfig().getThriftPort());
                pool = getNewPool(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster().getConnectionConfig()
                        .getThriftPort());
            }
        }
        throw new KunderaException("All hosts are down. please check servers manully.");
    }

    Selector getSelector(IThriftPool pool)
    {
        boolean success = false;
        while (!success)
        {
            try
            {
                success = true;
                if (pool != null)
                {
                    return Pelops.createSelector(getPoolName(pool));
                }
            }
            catch (TransportException te)
            {
                success = false;
                Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
                logger.warn("{} :{}  host appears to be down, trying for next ", nodes, ((CommonsBackedPool) pool)
                        .getCluster().getConnectionConfig().getThriftPort());
                pool = getNewPool(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster().getConnectionConfig()
                        .getThriftPort());
            }
        }
        throw new KunderaException("All hosts are down. please check servers manully.");

    }

    RowDeletor getRowDeletor(IThriftPool pool)
    {
        boolean success = false;
        while (!success)
        {
            try
            {
                success = true;
                if (pool != null)
                {
                    return Pelops.createRowDeletor(getPoolName(pool));
                }
            }
            catch (TransportException te)
            {
                success = false;
                Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
                logger.warn("{} :{}  host appears to be down, trying for next ", nodes, ((CommonsBackedPool) pool)
                        .getCluster().getConnectionConfig().getThriftPort());
                pool = getNewPool(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster().getConnectionConfig()
                        .getThriftPort());
            }
        }
        throw new KunderaException("All hosts are down. please check servers manully.");

    }

    private String getPoolName(IThriftPool pool)
    {
        org.scale7.cassandra.pelops.Cluster.Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
        String poolName = PelopsUtils.generatePoolName(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster()
                .getConnectionConfig().getThriftPort(), ((CommonsBackedPool) pool).getKeyspace());
        return poolName;
    }

    void releaseConnection(IPooledConnection conn)
    {
        if (conn != null)
        {
            conn.release();
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
        Properties props = KunderaMetadataManager.getPersistenceUnitMetadata(getPersistenceUnit()).getProperties();
        String keyspace = null;
        if (externalProperties != null)
        {
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        String poolName = PelopsUtils.generatePoolName(cassandraHost.getHost(), cassandraHost.getPort(), keyspace);
        Cluster cluster = new Cluster(cassandraHost.getHost(), new IConnection.Config(cassandraHost.getPort(), true,
                -1, PelopsUtils.getAuthenticationRequest(cassandraHost.getUser(), cassandraHost.getPassword())), false);
        Policy policy = PelopsUtils.getPoolConfigPolicy(cassandraHost);
        try
        {
            Pelops.addPool(poolName, cluster, keyspace, policy, null);
            hostPools.put(cassandraHost, Pelops.getDbConnPool(poolName));
            return true;
        }
        catch (TransportException e)
        {
            logger.warn("Node {} are still down ", cassandraHost.getHost());
            return false;
        }
    }

    private class PelopsLeastActiveBalancingPolcy implements LoadBalancingPolicy
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
            Iterator<Object> iterator = vals.iterator();
            Object concurrentConnectionPool = iterator.next();
            return concurrentConnectionPool;
        }

        private final class ShufflingCompare implements Comparator<Object>
        {
            public int compare(Object o1, Object o2)
            {
                Policy policy1 = ((CommonsBackedPool) ((IThriftPool) o1)).getPolicy();
                Policy policy2 = ((CommonsBackedPool) ((IThriftPool) o2)).getPolicy();

                return policy1.getMaxActivePerNode() - policy2.getMaxActivePerNode();
            }
        }
    }
}