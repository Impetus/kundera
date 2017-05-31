/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.client.mongo;

import com.impetus.client.crud.BaseTest;
import com.impetus.client.mongodb.MongoDBClientFactory;
import com.impetus.client.mongodb.MongoDBConstants;
import com.mongodb.DBDecoder;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoder;
import com.mongodb.DBEncoderFactory;
import com.mongodb.LazyDBDecoder;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import junit.framework.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests various configuration options for the MongoDB client factory.
 */
public class MongoDBConfigurationTest extends BaseTest
{

    /**
     * Tests configuration from the external 'kundera.client.property' file
     */
    @Test
    public void testConfigurationFromClientProperties()
    {
        Properties clientProperties = buildProperties(kv(MongoDBConstants.SAFE, "false"));

        MongoClientOptions options = buildOptions(clientProperties, null);

        Assert.assertEquals(WriteConcern.NORMAL, options.getWriteConcern());

        clientProperties = buildProperties(kv(MongoDBConstants.W, "0"), kv(MongoDBConstants.W_TIME_OUT, "1000"),
                kv(MongoDBConstants.FSYNC, "true"), kv(MongoDBConstants.J, "false"));

        options = buildOptions(clientProperties, null);

        WriteConcern wc = options.getWriteConcern();
        Assert.assertEquals(0, wc.getW());
        Assert.assertEquals(1000, wc.getWtimeout());
        Assert.assertEquals(true, wc.getFsync());
        Assert.assertEquals(false, wc.getJ());

        clientProperties = buildProperties(kv(MongoDBConstants.DB_DECODER_FACTORY, "com.mongodb.LazyDBDecoder.FACTORY"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(LazyDBDecoder.FACTORY, options.getDbDecoderFactory());

        clientProperties = buildProperties(kv(MongoDBConstants.DB_DECODER_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest$TestDBDecoderFactory"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(TestDBDecoderFactory.class, options.getDbDecoderFactory().getClass());

        clientProperties = buildProperties(kv(MongoDBConstants.DB_ENCODER_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest$TestDBEncoderFactory"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(TestDBEncoderFactory.class, options.getDbEncoderFactory().getClass());

        clientProperties = buildProperties(kv(MongoDBConstants.SOCKET_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest.INSECURE_SSL_SOCKET_FACTORY"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(INSECURE_SSL_SOCKET_FACTORY, options.getSocketFactory());

        clientProperties = buildProperties(kv(MongoDBConstants.SOCKET_FACTORY,
                "javax.net.ssl.SSLSocketFactory.getDefault()"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(SSLSocketFactory.getDefault().getClass(), options.getSocketFactory().getClass());

        clientProperties = buildProperties(kv(MongoDBConstants.AUTO_CONNECT_RETRY, "true"));

        options = buildOptions(clientProperties, null);

//        Assert.assertTrue(options.isAutoConnectRetry());

        clientProperties = buildProperties(kv(MongoDBConstants.MAX_AUTO_CONNECT_RETRY, "12"));

        options = buildOptions(clientProperties, null);

//        Assert.assertEquals(12L, options.getMaxAutoConnectRetryTime());

        clientProperties = buildProperties(kv(MongoDBConstants.CONNECTION_PER_HOST, "7"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(7, options.getConnectionsPerHost());

        clientProperties = buildProperties(kv(MongoDBConstants.CONNECT_TIME_OUT, "32"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(32, options.getConnectTimeout());

        clientProperties = buildProperties(kv(MongoDBConstants.MAX_WAIT_TIME, "41"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(41, options.getMaxWaitTime());

        clientProperties = buildProperties(kv(MongoDBConstants.TABCM, "3"));

        options = buildOptions(clientProperties, null);

        Assert.assertEquals(3, options.getThreadsAllowedToBlockForConnectionMultiplier());
    }

    /**
     * Tests configuration from external properties.
     */
    @Test
    public void testConfigurationFromExternalProperties()
    {
        Map<String, String> externalProperties = buildExternalProperties(kv(MongoDBConstants.SAFE, "false"));

        MongoClientOptions options = buildOptions(null, externalProperties);

        Assert.assertEquals(WriteConcern.NORMAL, options.getWriteConcern());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.W, "0"),
                kv(MongoDBConstants.W_TIME_OUT, "1000"), kv(MongoDBConstants.FSYNC, "true"),
                kv(MongoDBConstants.J, "false"));

        options = buildOptions(null, externalProperties);

        WriteConcern wc = options.getWriteConcern();
        Assert.assertEquals(0, wc.getW());
        Assert.assertEquals(1000, wc.getWtimeout());
        Assert.assertEquals(true, wc.getFsync());
        Assert.assertEquals(false, wc.getJ());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.DB_DECODER_FACTORY,
                "com.mongodb.LazyDBDecoder.FACTORY"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(LazyDBDecoder.FACTORY, options.getDbDecoderFactory());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.DB_DECODER_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest$TestDBDecoderFactory"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(TestDBDecoderFactory.class, options.getDbDecoderFactory().getClass());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.DB_ENCODER_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest$TestDBEncoderFactory"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(TestDBEncoderFactory.class, options.getDbEncoderFactory().getClass());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.SOCKET_FACTORY,
                "com.impetus.kundera.client.mongo.MongoDBConfigurationTest.INSECURE_SSL_SOCKET_FACTORY"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(INSECURE_SSL_SOCKET_FACTORY, options.getSocketFactory());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.SOCKET_FACTORY,
                "javax.net.ssl.SSLSocketFactory.getDefault()"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(SSLSocketFactory.getDefault().getClass(), options.getSocketFactory().getClass());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.AUTO_CONNECT_RETRY, "true"));

        options = buildOptions(null, externalProperties);

//        Assert.assertTrue(options.isAutoConnectRetry());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.MAX_AUTO_CONNECT_RETRY, "12"));

        options = buildOptions(null, externalProperties);

//        Assert.assertEquals(12L, options.getMaxAutoConnectRetryTime());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.CONNECTION_PER_HOST, "7"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(7, options.getConnectionsPerHost());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.CONNECT_TIME_OUT, "32"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(32, options.getConnectTimeout());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.MAX_WAIT_TIME, "41"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(41, options.getMaxWaitTime());

        externalProperties = buildExternalProperties(kv(MongoDBConstants.TABCM, "3"));

        options = buildOptions(null, externalProperties);

        Assert.assertEquals(3, options.getThreadsAllowedToBlockForConnectionMultiplier());
    }

    /**
     * Builds the options.
     * 
     * @param clientProperties
     *            the client properties
     * @param externalProperties
     *            the external properties
     * @return the mongo client options
     */
    private MongoClientOptions buildOptions(Properties clientProperties, Map<String, ?> externalProperties)
    {
        return new MongoDBClientFactory.PopulateMongoOptions(clientProperties, externalProperties).prepareBuilder()
                .build();
    }

    /**
     * Builds the properties.
     * 
     * @param keyValuePairs
     *            the key value pairs
     * @return the properties
     */
    private static Properties buildProperties(final KeyValue... keyValuePairs)
    {
        Properties properties = new Properties();
        for (KeyValue item : keyValuePairs)
        {
            properties.put(item.getKey(), item.getValue());
        }
        return properties;
    }

    /**
     * Builds the external properties.
     * 
     * @param keyValuePairs
     *            the key value pairs
     * @return the map
     */
    private static Map<String, String> buildExternalProperties(final KeyValue... keyValuePairs)
    {
        Map<String, String> properties = new HashMap<String, String>();
        for (KeyValue item : keyValuePairs)
        {
            properties.put(item.getKey(), item.getValue());
        }
        return properties;
    }

    /**
     * Kv.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @return the key value
     */
    private static KeyValue kv(String key, String value)
    {
        return new KeyValue(key, value);
    }

    /**
     * The Class KeyValue.
     */
    private static class KeyValue
    {

        /** The key. */
        private final String key;

        /** The value. */
        private final String value;

        /**
         * Instantiates a new key value.
         * 
         * @param key
         *            the key
         * @param value
         *            the value
         */
        public KeyValue(final String key, final String value)
        {
            this.key = key;
            this.value = value;
        }

        /**
         * Gets the key.
         * 
         * @return the key
         */
        public String getKey()
        {
            return key;
        }

        /**
         * Gets the value.
         * 
         * @return the value
         */
        public String getValue()
        {
            return value;
        }
    }

    /**
     * A factory for creating TestDBDecoder objects.
     */
    public static class TestDBDecoderFactory implements DBDecoderFactory
    {

        /*
         * (non-Javadoc)
         * 
         * @see com.mongodb.DBDecoderFactory#create()
         */
        @Override
        public DBDecoder create()
        {
            return null; // dummy
        }
    }

    /**
     * A factory for creating TestDBEncoder objects.
     */
    public static class TestDBEncoderFactory implements DBEncoderFactory
    {

        /*
         * (non-Javadoc)
         * 
         * @see com.mongodb.DBEncoderFactory#create()
         */
        @Override
        public DBEncoder create()
        {
            return null; // dummy
        }
    }

    /** The Constant INSECURE_SSL_SOCKET_FACTORY. */
    public static final SSLSocketFactory INSECURE_SSL_SOCKET_FACTORY;

    static
    {
        try
        {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] { new X509TrustManager()
            {
                public void checkClientTrusted(final X509Certificate[] x509Certificates, final String s)
                        throws CertificateException
                {
                }

                public void checkServerTrusted(final X509Certificate[] x509Certificates, final String s)
                        throws CertificateException
                {
                }

                public X509Certificate[] getAcceptedIssuers()
                {
                    return new X509Certificate[0];
                }
            } }, new SecureRandom());

            INSECURE_SSL_SOCKET_FACTORY = sslContext.getSocketFactory();
        }
        catch (Exception ex)
        {
            throw new AssertionError("Unexpected error", ex);
        }
    }

}
