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
package com.impetus.kundera.service.policy;

import java.util.Collection;

import com.google.common.collect.Iterables;

/**
 * RoundRobinBalancingPolicy returns pool using round robin algorithm.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public final class RoundRobinBalancingPolicy implements LoadBalancingPolicy
{

    private int counter;

    public RoundRobinBalancingPolicy()
    {
        counter = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.policy.LoadBalancingPolicy#getPool(java.util.Collection
     * , java.util.Set)
     */
    @Override
    public Object getPool(Collection<Object> pools)
    {
        try
        {
            return Iterables.get(pools, getAndIncrement(pools.size()));
        }
        catch (IndexOutOfBoundsException e)
        {
            return pools.iterator().next();
        }
    }

    private int getAndIncrement(int size)
    {
        int counterToReturn;
        synchronized (this)
        {
            if (counter >= 16384)
            {
                counter = 0;
            }
            counterToReturn = counter++;
        }

        return counterToReturn % size;
    }
}
