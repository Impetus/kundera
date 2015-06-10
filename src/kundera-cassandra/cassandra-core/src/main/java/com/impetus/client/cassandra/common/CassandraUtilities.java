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
package com.impetus.client.cassandra.common;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.persistence.PersistenceException;

import net.dataforte.cassandra.pool.HostFailoverPolicy;
import net.dataforte.cassandra.pool.PoolConfiguration;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.DateAccessor;

/**
 * Provides utilities methods
 * 
 * @author amresh.singh
 */
public class CassandraUtilities
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CassandraUtilities.class);

    public static String toUTF8(byte[] value)
    {
        return value == null ? null : new String(value, Charset.forName(Constants.CHARSET_UTF8));
    }

    public static String getKeyspace(final KunderaMetadata kunderaMetadata, String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadata.getProperties();
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        return keyspace;
    }

    public static ByteBuffer toBytes(Object value, Field f)
    {
        return toBytes(value, f.getType());

    }

    public static byte[] toBytes(final Object value)
    {
        if (value != null)
        {
            return toBytes(value, value.getClass()).array();
        }

        return null;
    }

    /**
     * @param value
     * @param f
     * @return
     */
    public static ByteBuffer toBytes(Object value, Class<?> clazz)
    {
        if (clazz.isAssignableFrom(String.class))
        {
            return UTF8Type.instance.decompose((String) value);
        }
        else if (clazz.equals(int.class) || clazz.isAssignableFrom(Integer.class))
        {
            return Int32Type.instance.decompose(Integer.parseInt(value.toString()));
        }
        else if (clazz.equals(long.class) || clazz.isAssignableFrom(Long.class))
        {
            return LongType.instance.decompose(Long.parseLong(value.toString()));
        }
        else if (clazz.equals(boolean.class) || clazz.isAssignableFrom(Boolean.class))
        {
            return BooleanType.instance.decompose(Boolean.valueOf(value.toString()));
        }
        else if (clazz.equals(double.class) || clazz.isAssignableFrom(Double.class))
        {
            return DoubleType.instance.decompose(Double.valueOf(value.toString()));
        }
        else if (clazz.isAssignableFrom(java.util.UUID.class))
        {
            return UUIDType.instance.decompose(UUID.fromString(value.toString()));
        }
        else if (clazz.equals(float.class) || clazz.isAssignableFrom(Float.class))
        {
            return FloatType.instance.decompose(Float.valueOf(value.toString()));
        }
        else if (clazz.isAssignableFrom(Date.class))
        {
            DateAccessor dateAccessor = new DateAccessor();
            return DateType.instance.decompose((Date) value);
        }
        else
        {
            if (value.getClass().isAssignableFrom(String.class))
            {
                value = PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz, value.toString());
            }
            return BytesType.instance.decompose(ByteBuffer.wrap(PropertyAccessorFactory.getPropertyAccessor(clazz)
                    .toBytes(value)));
        }
    }

    /**
     * Append columns.
     * 
     * @param builder
     *            the builder
     * @param columns
     *            the columns
     * @param selectQuery
     *            the select query
     * @param translator
     *            the translator
     */
    public static StringBuilder appendColumns(StringBuilder builder, List<String> columns, String selectQuery,
            CQLTranslator translator)
    {
        if (columns != null)
        {
            for (String column : columns)
            {
                translator.appendColumnName(builder, column);
                builder.append(",");
            }
        }
        if (builder.lastIndexOf(",") != -1)
        {
            builder.deleteCharAt(builder.length() - 1);
            selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMNS, builder.toString());
        }

        builder = new StringBuilder(selectQuery);
        return builder;
    }

    /**
     * Return name if Idcolumn for cql, returns {@CassandraConstants.CQL_KEY
     * 
     * 
     * 
     * 
     *     
     * } if user opted for
     * {@PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE
     * 
     * 
     * 
     * 
     *       
     * } otherwise returns
     * JPAColumnName of id attribute.
     * 
     * @param m
     * @param externalProperties
     * @return
     */
    public static String getIdColumnName(final KunderaMetadata kunderaMetadata, final EntityMetadata m,
            final Map<String, Object> externalProperties, final boolean isCql3Enabled)
    {
        // key for auto schema generation.
        String persistenceUnit = m.getPersistenceUnit();
        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String autoDdlOption = externalProperties != null ? (String) externalProperties
                .get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        if (autoDdlOption == null)
        {
            autoDdlOption = persistenceUnitMetadata != null ? persistenceUnitMetadata
                    .getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        }

        boolean containsBasicCollectionField = MetadataUtils.containsBasicElementCollectionField(m, kunderaMetadata);

        if (!StringUtils.isBlank(autoDdlOption) && !(isCql3Enabled || containsBasicCollectionField))
        {
            return CassandraConstants.CQL_KEY;
        }
        else
        {
            return ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        }
    }

     /**
     * 
     * @param host
     * @param port
     * @return
     */
    public static boolean verifyConnection(String host, int port)
    {
        Socket socket = null;
        try
        {
            socket = new Socket(host, port);
            socket.setReuseAddress(true);
            socket.setSoLinger(true, 0);
            boolean isConnected = socket.isConnected();
            return isConnected;
        }
        catch (UnknownHostException e)
        {
            logger.warn("{}:{} is still down", host, port);
            return false;
        }
        catch (IOException e)
        {
            logger.warn("{}:{} is still down", host, port);
            return false;
        }
        finally
        {
            try
            {
                if (socket != null)
                {
                    socket.close();
                }
            }
            catch (IOException e)
            {
                logger.warn("{}:{} is still down", host, port);
            }
        }
    }

    /**
     * Gets the pool config policy.
     * 
     * @param persistenceUnitMetadata
     *            the persistence unit metadata
     * @param puProperties
     * @return the pool config policy
     */
    public static PoolConfiguration setPoolConfigPolicy(
            com.impetus.client.cassandra.service.CassandraHost cassandraHost, PoolConfiguration prop)
    {
        int maxActivePerNode = cassandraHost.getMaxActive();
        int maxIdlePerNode = cassandraHost.getMaxIdle();
        int minIdlePerNode = cassandraHost.getMinIdle();
        int maxTotal = cassandraHost.getMaxTotal();
        boolean testOnBorrow = cassandraHost.isTestOnBorrow();
        boolean testWhileIdle = cassandraHost.isTestWhileIdle();
        boolean testOnConnect = cassandraHost.isTestOnConnect();
        boolean testOnReturn = cassandraHost.isTestOnReturn();
        int socketTimeOut = cassandraHost.getSocketTimeOut();
        int maxWaitInMilli = cassandraHost.getMaxWait();
        HostFailoverPolicy paramHostFailoverPolicy = cassandraHost.getHostFailoverPolicy();
        if (maxActivePerNode > 0)
        {
            prop.setInitialSize(maxActivePerNode);
            prop.setMaxActive(maxActivePerNode);
            logger.info("kundera.pool.size.max.active is set to: {}", maxActivePerNode);
        }
        else
        {
            logger.info("kundera.pool.size.max.active is set to default value: 100");
        }
        if (maxIdlePerNode > 0)
        {
            prop.setMaxIdle(maxIdlePerNode);
            logger.info("kundera.pool.size.max.idle is set to: {}", maxIdlePerNode);
        }
        else
        {
            logger.info("kundera.pool.size.max.idle is set to default value: 10");
        }
        if (minIdlePerNode > 0)
        {
            prop.setMinIdle(minIdlePerNode);
            logger.info("kundera.pool.size.min.idle is set to: {}", minIdlePerNode);
        }
        else
        {
            logger.info("kundera.pool.size.min.idle is set to default value: 100");
        }
        if (maxTotal > 0)
        {
            prop.setMaxActive(maxTotal);
            logger.info("kundera.pool.size.max.total is set to: {}", maxTotal);
        }
        if (cassandraHost.getUser() != null)
        {
            prop.setUsername(cassandraHost.getUser());
            prop.setPassword(cassandraHost.getPassword());
        }

        prop.setSocketTimeout(socketTimeOut);
        prop.setTestOnBorrow(testOnBorrow);
        prop.setTestOnConnect(testOnConnect);
        prop.setTestOnReturn(testOnReturn);
        prop.setTestWhileIdle(testWhileIdle);
        prop.setFailoverPolicy(paramHostFailoverPolicy);
        prop.setMaxWait(maxWaitInMilli);
        return prop;
    }
}
