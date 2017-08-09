/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.rethink;

import java.util.Map;
import java.util.Properties;

import com.impetus.client.rethink.query.RethinkDBEntityReader;
import com.impetus.client.rethink.schemamanager.RethinkDBSchemaManager;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

/**
 * A factory for creating RethinkDBClient objects.
 * 
 * @author karthikp.manchala
 */
public class RethinkDBClientFactory extends GenericClientFactory
{

    /** The Constant r. */
    private static final RethinkDB r = RethinkDB.r;

    /** The connection. */
    private Connection connection;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.ClientFactory#getSchemaManager(java.util.Map)
     */
    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if (schemaManager == null)
        {
            initializePropertyReader();
            setExternalProperties(puProperties);
            schemaManager = new RethinkDBSchemaManager(RethinkDBClientFactory.class.getName(), puProperties,
                    kunderaMetadata);
        }
        return schemaManager;
    }

    /**
     * Initialize property reader.
     */
    private void initializePropertyReader()
    {
        if (propertyReader == null)
        {
            propertyReader = new RethinkDBPropertyReader(externalProperties,
                    kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        // TODO Auto-generated method stub

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
        reader = new RethinkDBEntityReader(kunderaMetadata);
        setExternalProperties(puProperties);
        initializePropertyReader();
        PersistenceUnitMetadata pum = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties pumProps = pum.getProperties();

        if (puProperties != null)
        {
            pumProps.putAll(puProperties);
        }

        String host = pumProps.getProperty("kundera.nodes");
        String port = pumProps.getProperty("kundera.port");

        if (host == null || port == null)
        {
            throw new KunderaException("Hostname/IP or Port is null.");
        }

        connection = r.connection().hostname(host).port(Integer.parseInt(port)).connect();

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
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java.
     * lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new RethinkDBClient(kunderaMetadata, indexManager, reader, externalProperties, persistenceUnit,
                this.connection, this.clientMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.GenericClientFactory#isThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer(
     * java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        // TODO Auto-generated method stub

    }

}
