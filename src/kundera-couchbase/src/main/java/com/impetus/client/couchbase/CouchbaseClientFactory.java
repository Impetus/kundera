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
package com.impetus.client.couchbase;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.impetus.client.couchbase.query.CouchbaseEntityReader;
import com.impetus.client.couchbase.schemamanager.CouchbaseSchemaManager;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * A factory for creating CouchbaseClientFactory objects.
 * 
 * @author devender.yadav
 */
public class CouchbaseClientFactory extends GenericClientFactory
{

    /** The LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClientFactory.class);

    /** The cluster. */
    private CouchbaseCluster cluster;

    /** The bucket. */
    private Bucket bucket;

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
            schemaManager = new CouchbaseSchemaManager(CouchbaseClientFactory.class.getName(), puProperties,
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
            propertyReader = new CouchbasePropertyReader(externalProperties,
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
        indexManager.close();
        if (schemaManager != null)
        {
            schemaManager.dropSchema();
        }
        if (bucket != null)
        {
            LOGGER.info("Closing bucket " + bucket.name() + ".");
            CouchbaseBucketUtils.closeBucket(bucket);
            LOGGER.info("Closed bucket " + bucket.name() + ".");
        }
        if (cluster != null)
        {
            LOGGER.info("Closing connection to couchbase.");
            cluster.disconnect();
            LOGGER.info("Closed connection to couchbase.");
        }
        else
        {
            LOGGER.warn("Can't close connection to Couchbase, it was already disconnected");
        }
        externalProperties = null;
        schemaManager = null;
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
        reader = new CouchbaseEntityReader(kunderaMetadata);
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
        String keyspace = pumProps.getProperty("kundera.keyspace");

        if (host == null)
        {
            throw new KunderaException("Hostname/IP is null.");
        }

        if (keyspace == null)
        {
            throw new KunderaException("kundera.keyspace is null.");
        }
        cluster = CouchbaseCluster.create(splitHostNames(host));
        String password = ((CouchbasePropertyReader) propertyReader).csmd.getBucketProperty("bucket.password");
        bucket = CouchbaseBucketUtils.openBucket(cluster, keyspace, password);

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
        return new CouchbaseClient(kunderaMetadata, indexManager, reader, externalProperties, persistenceUnit,
                this.bucket, this.clientMetadata);
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
     * com.impetus.kundera.loader.GenericClientFactory#initializeLoadBalancer(
     * java.lang.String)
     */
    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {

    }

    /**
     * Split host names.
     *
     * @param hostNames
     *            the host names
     * @return the string[]
     */
    private String[] splitHostNames(String hostNames)
    {
        String[] hostArray = hostNames.split(Constants.COMMA);
        String[] hosts = new String[hostArray.length];
        for (int i = 0; i < hostArray.length; i++)
        {
            hosts[i] = hostArray[i].trim();
        }
        return hosts;
    }

}
