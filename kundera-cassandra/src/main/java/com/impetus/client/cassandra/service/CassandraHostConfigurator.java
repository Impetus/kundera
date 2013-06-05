package com.impetus.client.cassandra.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.dataforte.cassandra.pool.HostFailoverPolicy;

import org.apache.commons.lang.StringUtils;
import org.mortbay.jetty.servlet.HashSessionIdManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection.Server;
import com.impetus.kundera.service.Host;
import com.impetus.kundera.service.HostConfiguration;

public class CassandraHostConfigurator extends HostConfiguration
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CassandraHostConfigurator.class);

    private int retryDelay = 100;

    private Set<CassandraHost> downedHost;

    public CassandraHostConfigurator(Map externalProperties, CassandraSchemaMetadata csmd, String persistenceUnit)
    {
        super(externalProperties, csmd != null ? csmd.getConnectionServers() : new ArrayList<Server>(), persistenceUnit);
        String property = csmd.getConnectionProperties().getProperty(Constants.RETRY_DELAY);
        if (StringUtils.isNumeric(property))
        {
            retryDelay = Integer.parseInt(property);
        }
        downedHost = new HashSet<CassandraHost>();
    }

    protected CassandraHost[] buildHosts(List<Server> servers)
    {
        CassandraHost[] cassandraHosts = new CassandraHost[servers.size()];
        for (int i = 0; i < servers.size(); i++)
        {
            Server server = servers.get(i);
            String host = server.getHost();
            String portAsString = server.getPort();
            onValidation(host, portAsString);
            Properties serverProperties = server.getProperties();
            CassandraHost cassandraHost = new CassandraHost(host, Integer.parseInt(portAsString), serverProperties);
            setConfig(cassandraHost, serverProperties, null);
            cassandraHosts[i] = cassandraHost;
        }
        return cassandraHosts;
    }

    protected CassandraHost[] buildHosts(String hosts, String portAsString)
    {
        String[] hostVals = hosts.split(",");
        CassandraHost[] cassandraHosts = new CassandraHost[hostVals.length];
        for (int x = 0; x < hostVals.length; x++)
        {
            String host = hostVals[x].trim();
            onValidation(host, portAsString);
            int port = Integer.parseInt(portAsString);
            CassandraHost cassandraHost = port == CassandraHost.DEFAULT_PORT ? new CassandraHost(host)
                    : new CassandraHost(host, port);
            setConfig(cassandraHost, persistenceUnitMetadata.getProperties(), externalProperties);
            cassandraHosts[x] = cassandraHost;
        }
        return cassandraHosts;
    }

    @Override
    protected void setConfig(Host host, Properties props, Map puProperties)
    {
        CassandraHost cassandraHost = (CassandraHost) host;
        String maxActivePerNode = null;
        String maxIdlePerNode = null;
        String minIdlePerNode = null;
        String maxTotal = null;
        String testOnBorrow = null;
        String testWhileIdle = null;
        String testOnConnect = null;
        String testOnReturn = null;
        String socketTimeOut = null;
        if (puProperties != null)
        {
            maxActivePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            maxIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
            minIdlePerNode = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
            maxTotal = (String) puProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
            testOnBorrow = (String) puProperties.get(CassandraConstants.TEST_ON_BORROW);
            testOnConnect = (String) puProperties.get(CassandraConstants.TEST_ON_CONNECT);
            testOnReturn = (String) puProperties.get(CassandraConstants.TEST_ON_RETURN);
            testWhileIdle = (String) puProperties.get(CassandraConstants.TEST_WHILE_IDLE);
            socketTimeOut = (String) puProperties.get(CassandraConstants.SOCKET_TIMEOUT);
        }

        if (maxActivePerNode == null)
        {
            maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        if (maxIdlePerNode == null)
        {
            maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
        }
        if (minIdlePerNode == null)
        {
            minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
        }
        if (maxTotal == null)
        {
            maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        }
        try
        {
            if (!StringUtils.isEmpty(maxActivePerNode))
            {
                cassandraHost.setInitialSize(Integer.parseInt(maxActivePerNode));
            }

            if (!StringUtils.isEmpty(maxIdlePerNode))
            {
                cassandraHost.setMaxIdle(Integer.parseInt(maxIdlePerNode));
            }

            if (!StringUtils.isEmpty(minIdlePerNode))
            {
                cassandraHost.setMinIdle(Integer.parseInt(minIdlePerNode));
            }

            if (!StringUtils.isEmpty(maxTotal))
            {
                cassandraHost.setMaxActive(Integer.parseInt(maxTotal));
            }

            if (testOnBorrow == null)
            {
                testOnBorrow = props.getProperty(CassandraConstants.TEST_ON_BORROW);
            }
            if (testOnConnect == null)
            {
                testOnConnect = props.getProperty(CassandraConstants.TEST_ON_CONNECT);
            }
            if (testOnReturn == null)
            {
                testOnReturn = props.getProperty(CassandraConstants.TEST_ON_RETURN);
            }
            if (testWhileIdle == null)
            {
                testWhileIdle = props.getProperty(CassandraConstants.TEST_WHILE_IDLE);
            }
            if (socketTimeOut == null)
            {
                socketTimeOut = props.getProperty(CassandraConstants.SOCKET_TIMEOUT);
            }

            cassandraHost.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
            cassandraHost.setTestOnConnect(Boolean.parseBoolean(testOnConnect));
            cassandraHost.setTestOnReturn(Boolean.parseBoolean(testOnReturn));
            cassandraHost.setTestWhileIdle(Boolean.parseBoolean(testWhileIdle));
            cassandraHost.setHostFailoverPolicy(getFailoverPolicy(props.getProperty(Constants.FAILOVER_POLICY)));
            cassandraHost.setRetryHost(Boolean.parseBoolean(props.getProperty(Constants.RETRY)));

            if (!StringUtils.isEmpty(socketTimeOut))
            {
                cassandraHost.setSocketTimeout(Integer.parseInt(socketTimeOut));
            }
            else
            {
                cassandraHost.setSocketTimeout(CassandraHost.DEFAULT_SHOCKET_TIMEOUT);
            }

        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property couldn't be parsed. Default pool policy would be used");
            cassandraHost = null;
        }
    }

    public CassandraHost[] getCassandraHosts()
    {
        return (CassandraHost[]) hosts;
    }

    private HostFailoverPolicy getFailoverPolicy(String failoverOption)
    {
        if (failoverOption != null)
        {
            if (Constants.FAIL_FAST.equals(failoverOption))
            {
                return HostFailoverPolicy.FAIL_FAST;
            }
            else if (Constants.ON_FAIL_TRY_ALL_AVAILABLE.equals(failoverOption))
            {
                return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
            }
            else if (Constants.ON_FAIL_TRY_ONE_NEXT_AVAILABLE.equals(failoverOption))
            {
                return HostFailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE;
            }
            else
            {
                logger.warn("Not provided valid failover policy, using default");
                return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
            }
        }
        return HostFailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    }

    public Set<CassandraHost> getDownedHost()
    {
        return downedHost;
    }

    public void addHostToDownedHost(CassandraHost cassandraHost)
    {
        downedHost.add(cassandraHost);
    }

    public int getRetryDelay()
    {
        return retryDelay;
    }

    public CassandraHost getCassandraHost(String host, int port)
    {
        for (Host cassandraHost : hosts)
        {
            if (((CassandraHost) cassandraHost).equals(new CassandraHost(host, port)))
            {
                return (CassandraHost) cassandraHost;
            }
        }
        return null;
    }
}
