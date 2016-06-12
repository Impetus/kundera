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
package com.impetus.client.es;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author vivek.mishra Client factory implementation for elastic search.
 */
public class ESClientFactory extends GenericClientFactory
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        this.externalProperties = null;
        Object connection = getConnectionPoolOrConnection();

        if (connection != null)
        {
            ((TransportClient) connection).close();

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        this.externalProperties = puProperties;

        this.propertyReader = new ESClientPropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit()));
        propertyReader.read(getPersistenceUnit());
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
        PersistenceUnitMetadata persistenceUnitMetadata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();

        String host = externalProperties != null ? (String) externalProperties.get(PersistenceProperties.KUNDERA_NODES)
                : null;
        String port = externalProperties != null ? (String) externalProperties.get(PersistenceProperties.KUNDERA_PORT)
                : null;

        if (host == null)
        {
            host = props.getProperty(PersistenceProperties.KUNDERA_NODES);
        }

        if (port == null)
        {
            port = props.getProperty(PersistenceProperties.KUNDERA_PORT);
        }

        String[] hosts = getHosts(host);

        Properties properties = ((ESClientPropertyReader) propertyReader).getConnectionProperties();

        Builder builder = Settings.settingsBuilder();

        builder.put("client.transport.sniff", true);

        if (properties != null)
        {
            builder.put(properties);
        }

        Settings settings = builder.build();

        TransportClient client = TransportClient.builder().settings(settings).build();

        for (String h : hosts)
        {
            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(h, new Integer(port))));
        }

        return client;
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
        return new ESClient(this, ((TransportClient) getConnectionPoolOrConnection()), this.externalProperties,
                kunderaMetadata, persistenceUnit);
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

    }

    private String[] getHosts(final String host)
    {
        return host.split(",");
    }

}
