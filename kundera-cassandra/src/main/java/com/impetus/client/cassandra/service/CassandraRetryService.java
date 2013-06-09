package com.impetus.client.cassandra.service;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javassist.expr.Instanceof;

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

import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;
import com.impetus.kundera.service.policy.RetryService;

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
