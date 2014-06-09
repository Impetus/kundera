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

import org.scale7.cassandra.pelops.SimpleConnectionAuthenticator;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static String generatePoolName(String node, int port, String keyspace)
    {
        return node + ":" + port + ":" + keyspace;
    }

/*    *//**
     * Gets the pool config policy.
     * 
     * @param persistenceUnitMetadata
     *            the persistence unit metadata
     * @param puProperties
     * @return the pool config policy
     */
    public static Policy getPoolConfigPolicy(com.impetus.client.cassandra.service.CassandraHost cassandraHost)
    {
        Policy policy = new Policy();
        if (cassandraHost.getMaxActive() > 0)
        {
            policy.setMaxActivePerNode(cassandraHost.getMaxActive());
        }
        if (cassandraHost.getMaxIdle() > 0)
        {
            policy.setMaxIdlePerNode(cassandraHost.getMaxIdle());
        }
        if (cassandraHost.getMinIdle() > 0)
        {
            policy.setMinIdlePerNode(cassandraHost.getMinIdle());
        }
        if (cassandraHost.getMaxTotal() > 0)
        {
            policy.setMaxTotal(cassandraHost.getMaxTotal());
        }
        return policy;
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
    public static SimpleConnectionAuthenticator getAuthenticationRequest(String userName, String password)
    {
        SimpleConnectionAuthenticator authenticator = null;
        if (userName != null || password != null)
        {
            authenticator = new SimpleConnectionAuthenticator(userName, password);
        }
        return authenticator;
    }


    /**
     * 
     * @param pool
     * @return
     */
    public static String getPoolName(IThriftPool pool)
    {
        org.scale7.cassandra.pelops.Cluster.Node[] nodes = ((CommonsBackedPool) pool).getCluster().getNodes();
        String poolName = PelopsUtils.generatePoolName(nodes[0].getAddress(), ((CommonsBackedPool) pool).getCluster()
                .getConnectionConfig().getThriftPort(), ((CommonsBackedPool) pool).getKeyspace());
        return poolName;
    }


}
