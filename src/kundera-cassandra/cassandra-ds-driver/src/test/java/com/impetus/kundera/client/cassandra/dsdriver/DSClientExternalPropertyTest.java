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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.HostDistance;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * 
 * @author shaheed.hussain Test case to check the ds client properties set via a
 *         xml file.
 * 
 *         {@link DSClientFactory}
 */

public class DSClientExternalPropertyTest
{

    private EntityManagerFactory emf;

    private final String keyspaceName = "KunderaExamples";

    private final String _PU = "external_pu";

    private Map propertyMap = new HashMap();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(keyspaceName);
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        // emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // emf.close();
        CassandraCli.dropKeySpace(keyspaceName);
    }

    /**
     * Test to check external xml properties in case of
     * RoundRobinPolicy,ExponentialReconnectionPolicy,FallthroughRetryPolicy
     * with baseDelayMs,maxDelayMs available in the external xml file
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    @Test
    public void withAllPropertyTest() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException
    {

        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        DSClientFactory ds = new DSClientFactory();
        final String RRP = "com.datastax.driver.core.policies.RoundRobinPolicy";
        final String ERP = "com.datastax.driver.core.policies.ExponentialReconnectionPolicy";
        final String DCRP = "com.datastax.driver.core.policies.FallthroughRetryPolicy";
        Properties connectionProperties = initialize(ds);

        ds.initialize(propertyMap);
        Object conn = ds.createPoolOrConnection();
        Cluster cluster = (Cluster) conn;

        HostDistance distance = HostDistance.LOCAL;

        Configuration configuration = cluster.getConfiguration();

        Assert.assertEquals(configuration.getSocketOptions().getReadTimeoutMillis(), 110000);
        Assert.assertEquals(configuration.getSocketOptions().getKeepAlive().booleanValue(), false);
        Assert.assertEquals(configuration.getSocketOptions().getReceiveBufferSize().intValue(), 12);
        Assert.assertEquals(configuration.getSocketOptions().getReuseAddress().booleanValue(), true);
        Assert.assertEquals(configuration.getSocketOptions().getSendBufferSize().intValue(), 11);
        Assert.assertEquals(configuration.getSocketOptions().getSoLinger().intValue(), 10);
        Assert.assertEquals(configuration.getSocketOptions().getTcpNoDelay().booleanValue(), true);

        Assert.assertEquals(configuration.getPoolingOptions().getCoreConnectionsPerHost(distance), 5);
        Assert.assertEquals(configuration.getPoolingOptions().getMaxConnectionsPerHost(distance), 12);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMaxSimultaneousRequestsPerConnectionThreshold(distance), 200);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMinSimultaneousRequestsPerConnectionThreshold(distance), 65);

        Assert.assertEquals(configuration.getPolicies().getLoadBalancingPolicy().getClass().getName(), RRP);
        Assert.assertEquals(configuration.getPolicies().getReconnectionPolicy().getClass().getName(), ERP);
        Assert.assertEquals(configuration.getPolicies().getRetryPolicy().getClass().getName(), DCRP);

        Assert.assertEquals(connectionProperties.getProperty("baseDelayMs"), "11000");
        Assert.assertEquals(connectionProperties.getProperty("maxDelayMs"), "13000");

    }

    private Properties initialize(DSClientFactory ds) throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException
    {
        Properties connectionProperties = CassandraPropertyReader.csmd.getConnectionProperties();

        Method m = GenericClientFactory.class.getDeclaredMethod("setKunderaMetadata", KunderaMetadata.class);
        if (!m.isAccessible())
        {
            m.setAccessible(true);
        }

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        m.invoke(ds, kunderaMetadata);

        m = GenericClientFactory.class.getDeclaredMethod("setPersistenceUnit", String.class);
        if (!m.isAccessible())
        {
            m.setAccessible(true);
        }

        m.invoke(ds, _PU);

        m = GenericClientFactory.class.getDeclaredMethod("setExternalProperties", Map.class);
        if (!m.isAccessible())
        {
            m.setAccessible(true);
        }

        m.invoke(ds, propertyMap);
        return connectionProperties;
    }

    /**
     * Test to check external xml properties in case of
     * DCAwareRoundRobinPolicy,ConstantReonnectionPolicy
     * ,DowngradingConsistencyRetryPolicy with
     * localdc,usedHostsPerRemoteDc,constantDelayMs available in the external
     * xml file
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */

    @Test
    public void withPropertyTest2() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException
    {
        propertyMap.put("kundera.client.property", "dsclienttest2.xml");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        DSClientFactory ds = new DSClientFactory();
        final String DRRP = "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy";
        final String CRP = "com.datastax.driver.core.policies.ConstantReconnectionPolicy";
        final String DCRP = "com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy";
        Properties connectionProperties = initialize(ds);

        ds.initialize(propertyMap);
        Object conn = ds.createPoolOrConnection();
        Cluster cluster = (Cluster) conn;

        HostDistance distance = HostDistance.LOCAL;

        Configuration configuration = cluster.getConfiguration();

        Assert.assertEquals(configuration.getSocketOptions().getReadTimeoutMillis(), 110000);
        Assert.assertEquals(configuration.getSocketOptions().getKeepAlive().booleanValue(), false);
        Assert.assertEquals(configuration.getSocketOptions().getReceiveBufferSize().intValue(), 12);
        Assert.assertEquals(configuration.getSocketOptions().getReuseAddress().booleanValue(), true);
        Assert.assertEquals(configuration.getSocketOptions().getSendBufferSize().intValue(), 11);
        Assert.assertEquals(configuration.getSocketOptions().getSoLinger().intValue(), 10);
        Assert.assertEquals(configuration.getSocketOptions().getTcpNoDelay().booleanValue(), true);

        Assert.assertEquals(configuration.getPoolingOptions().getCoreConnectionsPerHost(distance), 5);
        Assert.assertEquals(configuration.getPoolingOptions().getMaxConnectionsPerHost(distance), 12);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMaxSimultaneousRequestsPerConnectionThreshold(distance), 200);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMinSimultaneousRequestsPerConnectionThreshold(distance), 65);

        Assert.assertEquals(configuration.getPolicies().getLoadBalancingPolicy().getClass().getName(), DRRP);
        Assert.assertEquals(configuration.getPolicies().getReconnectionPolicy().getClass().getName(), CRP);
        Assert.assertEquals(configuration.getPolicies().getRetryPolicy().getClass().getName(), DCRP);

        Assert.assertEquals(connectionProperties.getProperty("constantDelayMs"), "110000");
        Assert.assertEquals(connectionProperties.getProperty("localdc"), "dc1");
        Assert.assertEquals(connectionProperties.getProperty("usedHostsPerRemoteDc"), "2");
        Assert.assertEquals(connectionProperties.getProperty("isTokenAware"), "true");
        Assert.assertEquals(connectionProperties.getProperty("isLoggingRetry"), "true");

        emf.close();

    }

    /**
     * Test to check external xml properties in case of
     * RoundRobinPolicy,ExponentialReconnectionPolicy,FallthroughRetryPolicy
     * with baseDelayMs,maxDelayMs missing from external xml file
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */

    @Test
    public void missingPropertytest() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException
    {

        propertyMap.put("kundera.client.property", "DSClientTestWithMissingProperties.xml");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        DSClientFactory ds = new DSClientFactory();
        final String RRP = "com.datastax.driver.core.policies.RoundRobinPolicy";
        final String ERP = "com.datastax.driver.core.policies.ExponentialReconnectionPolicy";
        final String DCRP = "com.datastax.driver.core.policies.FallthroughRetryPolicy";
        Properties connectionProperties = initialize(ds);

        ds.initialize(propertyMap);
        Object conn = ds.createPoolOrConnection();
        Cluster cluster = (Cluster) conn;

        HostDistance distance = HostDistance.LOCAL;

        Configuration configuration = cluster.getConfiguration();

        Assert.assertEquals(configuration.getSocketOptions().getReadTimeoutMillis(), 110000);
        Assert.assertEquals(configuration.getSocketOptions().getKeepAlive().booleanValue(), false);
        Assert.assertEquals(configuration.getSocketOptions().getReceiveBufferSize().intValue(), 12);
        Assert.assertEquals(configuration.getSocketOptions().getReuseAddress().booleanValue(), true);
        Assert.assertEquals(configuration.getSocketOptions().getSendBufferSize().intValue(), 11);
        Assert.assertEquals(configuration.getSocketOptions().getSoLinger().intValue(), 10);
        Assert.assertEquals(configuration.getSocketOptions().getTcpNoDelay().booleanValue(), true);

        Assert.assertEquals(configuration.getPoolingOptions().getCoreConnectionsPerHost(distance), 5);
        Assert.assertEquals(configuration.getPoolingOptions().getMaxConnectionsPerHost(distance), 12);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMaxSimultaneousRequestsPerConnectionThreshold(distance), 200);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMinSimultaneousRequestsPerConnectionThreshold(distance), 65);

        Assert.assertEquals(configuration.getPolicies().getLoadBalancingPolicy().getClass().getName(), RRP);
        Assert.assertEquals(configuration.getPolicies().getReconnectionPolicy().getClass().getName(), ERP);
        Assert.assertEquals(configuration.getPolicies().getRetryPolicy().getClass().getName(), DCRP);

        Assert.assertEquals(connectionProperties.getProperty("baseDelayMs"), null);
        Assert.assertEquals(connectionProperties.getProperty("maxDelayMs"), null);
        emf.close();

    }

    /**
     * Test to check external xml properties in case of
     * DCAwareRoundRobinPolicy,ConstantReonnectionPolicy
     * ,DowngradingConsistencyRetryPolicy with
     * localdc,usedHostsPerRemoteDc,constantDelayMs missing from the external
     * xml file
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */

    @Test
    public void missingPropertyTest2() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException
    {

        propertyMap.put("kundera.client.property", "DSClientTestWithMissingProperties2.xml");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        DSClientFactory ds = new DSClientFactory();
        final String DRRP = "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy";
        final String CRP = "com.datastax.driver.core.policies.ConstantReconnectionPolicy";
        final String DCRP = "com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy";
        Properties connectionProperties = initialize(ds);

        ds.initialize(propertyMap);
        Object conn = ds.createPoolOrConnection();
        Cluster cluster = (Cluster) conn;

        HostDistance distance = HostDistance.LOCAL;

        Configuration configuration = cluster.getConfiguration();

        Assert.assertEquals(configuration.getSocketOptions().getReadTimeoutMillis(), 110000);
        Assert.assertEquals(configuration.getSocketOptions().getKeepAlive().booleanValue(), false);
        Assert.assertEquals(configuration.getSocketOptions().getReceiveBufferSize().intValue(), 12);
        Assert.assertEquals(configuration.getSocketOptions().getReuseAddress().booleanValue(), true);
        Assert.assertEquals(configuration.getSocketOptions().getSendBufferSize().intValue(), 11);
        Assert.assertEquals(configuration.getSocketOptions().getSoLinger().intValue(), 10);
        Assert.assertEquals(configuration.getSocketOptions().getTcpNoDelay().booleanValue(), true);

        Assert.assertEquals(configuration.getPoolingOptions().getCoreConnectionsPerHost(distance), 5);
        Assert.assertEquals(configuration.getPoolingOptions().getMaxConnectionsPerHost(distance), 12);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMaxSimultaneousRequestsPerConnectionThreshold(distance), 200);
        Assert.assertEquals(configuration.getPoolingOptions()
                .getMinSimultaneousRequestsPerConnectionThreshold(distance), 65);

        Assert.assertEquals(configuration.getPolicies().getLoadBalancingPolicy().getClass().getName(), DRRP);
        Assert.assertEquals(configuration.getPolicies().getReconnectionPolicy().getClass().getName(), CRP);
        Assert.assertEquals(configuration.getPolicies().getRetryPolicy().getClass().getName(), DCRP);

        Assert.assertEquals(connectionProperties.getProperty("constantDelayMs"), null);

        emf.close();

    }

}
