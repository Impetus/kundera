/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * CouchDB client factory.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBClientFactory extends GenericClientFactory
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBClientFactory.class);

    /** The http client. */
    private HttpClient httpClient;

    /** The http host. */
    private HttpHost httpHost;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        setExternalProperties(puProperties);
        if (schemaManager == null)
        {
            initializePropertyReader();
            schemaManager = new CouchDBSchemaManager(this.getClass().getName(), puProperties, this.kunderaMetadata);
        }
        return schemaManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        if (indexManager != null)
        {
            indexManager.close();
        }
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
        schemaManager = null;
        externalProperties = null;
        httpClient.getConnectionManager().shutdown();

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> externalProperty)
    {
        reader = new CouchDBEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(externalProperty);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
     */
    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                getPersistenceUnit());

        Properties props = puMetadata.getProperties();
        String contactNode = null;
        String defaultPort = null;
        String keyspace = null;
        String poolSize = null;
        String userName = null;
        String password = null;
        String maxConnections = null;
        if (externalProperties != null)
        {
            contactNode = (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES);
            defaultPort = (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT);
            keyspace = (String) externalProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            poolSize = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
            userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
            maxConnections = (String) externalProperties.get(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);
        }
        if (contactNode == null)
        {
            contactNode = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        }
        if (defaultPort == null)
        {
            defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        }
        if (keyspace == null)
        {
            keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        }
        if (poolSize == null)
        {
            poolSize = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        }
        if (userName == null)
        {
            userName = props.getProperty(PersistenceProperties.KUNDERA_USERNAME);
            password = props.getProperty(PersistenceProperties.KUNDERA_PASSWORD);
        }

        onValidation(contactNode, defaultPort);
        try
        {
            SchemeSocketFactory ssf = null;
            ssf = PlainSocketFactory.getSocketFactory();
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme(CouchDBConstants.PROTOCOL, Integer.parseInt(defaultPort), ssf));
            PoolingClientConnectionManager ccm = new PoolingClientConnectionManager(schemeRegistry);
            httpClient = new DefaultHttpClient(ccm);
            httpHost = new HttpHost(contactNode, Integer.parseInt(defaultPort), CouchDBConstants.PROTOCOL);
            httpClient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");
            // httpclient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,
            // props.getSocketTimeout());

            // httpclient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
            // props.getConnectionTimeout());
            if (!StringUtils.isBlank(maxConnections))
            {
                ccm.setMaxTotal(Integer.parseInt(maxConnections));
                ccm.setDefaultMaxPerRoute(Integer.parseInt(maxConnections));
            }
            // basic authentication
            if (userName != null && password != null)
            {
                ((AbstractHttpClient) httpClient).getCredentialsProvider().setCredentials(
                        new AuthScope(contactNode, Integer.parseInt(defaultPort)),
                        new UsernamePasswordCredentials(userName, password));
            }
            // request interceptor
            ((DefaultHttpClient) httpClient).addRequestInterceptor(new HttpRequestInterceptor()
            {
                public void process(final HttpRequest request, final HttpContext context) throws IOException
                {
                    if (logger.isInfoEnabled())
                    {
                        RequestLine requestLine = request.getRequestLine();
                        logger.info(">> " + requestLine.getMethod() + " " + URI.create(requestLine.getUri()).getPath());
                    }
                }
            });
            // response interceptor
            ((DefaultHttpClient) httpClient).addResponseInterceptor(new HttpResponseInterceptor()
            {
                public void process(final HttpResponse response, final HttpContext context) throws IOException
                {
                    if (logger.isInfoEnabled())
                        logger.info("<< Status: " + response.getStatusLine().getStatusCode());
                }
            });
        }
        catch (Exception e)
        {
            logger.error("Error Creating HTTP client, caoused by {}. ", e);
            throw new IllegalStateException(e);
        }
        return httpClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java
     * .lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new CouchDBClient(this, httpClient, httpHost, reader, persistenceUnit, externalProperties,
                clientMetadata, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer
     * (java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    /**
     * Initialize property reader.
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new CouchDBPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    /**
     * Gets the index manager.
     * 
     * @return the index manager
     */
    public IndexManager getIndexManager()
    {
        return indexManager;
    }
}