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

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;
import com.impetus.kundera.service.policy.RetryService;

/**
 * Cassandra retry service, retries for downed cassandra server with a fix
 * delay, and add it to hostspool map when it up.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class CassandraRetryService extends RetryService
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);

    private LinkedBlockingQueue<CassandraHost> downedHostQueue;

    private ClientFactory clientFactory;

    public CassandraRetryService(HostConfiguration configuration, ClientFactory clientFactory)
    {
        super(((CassandraHostConfiguration) configuration).getRetryDelay());
        downedHostQueue = new LinkedBlockingQueue<CassandraHost>();
        this.clientFactory = clientFactory;
        sf = executor.scheduleWithFixedDelay(new RetryRunner(), this.retryDelayInSeconds, this.retryDelayInSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    protected boolean verifyConnection(Host host)
    {
        return PelopsUtils.verifyConnection(host.getHost(), host.getPort());
    }

    class RetryRunner implements Runnable
    {

        @Override
        public void run()
        {
            if (!downedHostQueue.isEmpty())
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
            Iterator<CassandraHost> iter = downedHostQueue.iterator();
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
                    if (clientFactory instanceof ThriftClientFactory)
                    {
                        ((ThriftClientFactory) clientFactory).addCassandraHost(host);
                    }
                    else
                    {
                        ((PelopsClientFactory) clientFactory).addCassandraHost(host);
                    }
                    iter.remove();
                }
            }
        }
    }

    public void add(final CassandraHost cassandraHost)
    {
        downedHostQueue.add(cassandraHost);

        // schedule a check of this host immediately,
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                if (verifyConnection(cassandraHost))
                {
                    if (clientFactory instanceof ThriftClientFactory
                            && ((ThriftClientFactory) clientFactory).addCassandraHost(cassandraHost)
                            || clientFactory instanceof PelopsClientFactory
                            && ((PelopsClientFactory) clientFactory).addCassandraHost(cassandraHost))
                    {
                        downedHostQueue.remove(cassandraHost);
                    }
                }
            }
        });
    }

    @Override
    public void shutdown()
    {
        downedHostQueue.clear();
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
