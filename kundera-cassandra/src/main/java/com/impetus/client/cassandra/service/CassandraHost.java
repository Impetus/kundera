package com.impetus.client.cassandra.service;

import java.util.Properties;

import net.dataforte.cassandra.pool.HostFailoverPolicy;

import com.impetus.kundera.service.Host;

public class CassandraHost implements Host
{

    public static final int DEFAULT_PORT = 9160;

    public static final int DEFAULT_SHOCKET_TIMEOUT = 120000;

    public static final int DEFAULT_MAX_ACTIVE = 30;

    // Cap on the number of "idle" instances in the pool.
    public static final int DEFAULT_MAX_IDLE = 10;

    // Minimum number of idle objects to maintain in each of the nodes.
    public static final int DEFAULT_MIN_IDLE = 5;

    // Cap on the total number of instances from all nodes combined.
    public static final int DEFAULT_MAX_TOTAL = 50;

    private int maxActive;

    // Cap on the number of "idle" instances in the pool.
    private int maxIdle;

    // Minimum number of idle objects to maintain in each of the nodes.
    private int minIdle;

    // Cap on the total number of instances from all nodes combined.
    private int maxTotal;

    private String host;

    private int port;

    private Properties properties = new Properties();

    private int initialSize;

    private boolean testOnBorrow;

    private boolean testOnConnect;

    private boolean testOnReturn;

    private boolean testWhileIdle;

    private int socketTimeOut;

    private HostFailoverPolicy hostFailoverPolicy;

    private boolean retryHost;

    public CassandraHost(String host)
    {
        this.host = host;
        this.port = DEFAULT_PORT;
    }

    public CassandraHost(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public CassandraHost(String host, int port, Properties hostProperties)
    {
        this(host, port);
        this.properties = hostProperties;
    }

    @Override
    public String getHost()
    {
        return host;
    }

    @Override
    public void setHost(String host)
    {
        this.host = host;

    }

    @Override
    public void setPort(int port)
    {
        this.port = port;
    }

    @Override
    public int getPort()
    {
        return port;
    }

    @Override
    public Properties getProperties()
    {
        return properties;
    }

    @Override
    public void setProperties(Properties hostProperties)
    {
        this.properties = hostProperties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof CassandraHost))
        {
            return false;
        }
        CassandraHost other = (CassandraHost) obj;
        return other.host.equals(this.host) && other.port == this.port;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return host;
    }

    public void setInitialSize(int initialSize)
    {
        this.initialSize = initialSize;
    }

    public void setTestOnBorrow(boolean testOnBorrow)
    {
        this.testOnBorrow = testOnBorrow;
    }

    public void setTestOnConnect(boolean testOnConnect)
    {
        this.testOnConnect = testOnConnect;
    }

    public void setTestOnReturn(boolean testOnReturn)
    {
        this.testOnReturn = testOnReturn;
    }

    public void setTestWhileIdle(boolean testWhileIdle)
    {
        this.testWhileIdle = testWhileIdle;
    }

    public void setSocketTimeout(int socketTimeOut)
    {
        this.socketTimeOut = socketTimeOut;
    }

    /**
     * @return the maxTotal
     */
    public int getMaxTotal()
    {
        return maxTotal;
    }

    /**
     * @param maxTotal
     *            the maxTotal to set
     */
    public void setMaxTotal(int maxTotal)
    {
        this.maxTotal = maxTotal;
    }

    /**
     * @return the maxActive
     */
    public int getMaxActive()
    {
        return maxActive;
    }

    /**
     * @return the maxIdle
     */
    public int getMaxIdle()
    {
        return maxIdle;
    }

    /**
     * @return the minIdle
     */
    public int getMinIdle()
    {
        return minIdle;
    }

    /**
     * @param maxActive
     *            the maxActive to set
     */
    public void setMaxActive(int maxActive)
    {
        this.maxActive = maxActive;
    }

    /**
     * @param maxIdle
     *            the maxIdle to set
     */
    public void setMaxIdle(int maxIdle)
    {
        this.maxIdle = maxIdle;
    }

    /**
     * @param minIdle
     *            the minIdle to set
     */
    public void setMinIdle(int minIdle)
    {
        this.minIdle = minIdle;
    }

    /**
     * @return the socketTimeOut
     */
    public int getSocketTimeOut()
    {
        return socketTimeOut;
    }

    /**
     * @param socketTimeOut
     *            the socketTimeOut to set
     */
    public void setSocketTimeOut(int socketTimeOut)
    {
        this.socketTimeOut = socketTimeOut;
    }

    /**
     * @return the initialSize
     */
    public int getInitialSize()
    {
        return initialSize;
    }

    /**
     * @return the testOnBorrow
     */
    public boolean isTestOnBorrow()
    {
        return testOnBorrow;
    }

    /**
     * @return the testOnConnect
     */
    public boolean isTestOnConnect()
    {
        return testOnConnect;
    }

    /**
     * @return the testOnReturn
     */
    public boolean isTestOnReturn()
    {
        return testOnReturn;
    }

    /**
     * @return the testWhileIdle
     */
    public boolean isTestWhileIdle()
    {
        return testWhileIdle;
    }

    public HostFailoverPolicy getHostFailoverPolicy()
    {
        return this.hostFailoverPolicy;
    }

    public void setHostFailoverPolicy(HostFailoverPolicy hostFailoverPolicy)
    {
        this.hostFailoverPolicy = hostFailoverPolicy;
    }

    public boolean isRetryHost()
    {
        return retryHost;
    }

    public void setRetryHost(boolean retryHost)
    {
        this.retryHost = retryHost;
    }
}
