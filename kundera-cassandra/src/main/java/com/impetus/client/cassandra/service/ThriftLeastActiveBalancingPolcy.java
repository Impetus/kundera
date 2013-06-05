package com.impetus.client.cassandra.service;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.dataforte.cassandra.pool.ConnectionPool;
import net.dataforte.cassandra.pool.PoolConfiguration;

import com.google.common.collect.Lists;
import com.impetus.kundera.service.policy.LeastActiveBalancingPolicy;

public class ThriftLeastActiveBalancingPolcy extends LeastActiveBalancingPolicy
{

    @Override
    public Object getPool(Collection<Object> pools, Set<Object> excludeHosts)
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
            PoolConfiguration props1 = ((ConnectionPool) o1).getPoolProperties();
            PoolConfiguration props2 = ((ConnectionPool) o2).getPoolProperties();
            return props1.getMaxActive() | -props2.getMaxActive();
        }
    }
}
