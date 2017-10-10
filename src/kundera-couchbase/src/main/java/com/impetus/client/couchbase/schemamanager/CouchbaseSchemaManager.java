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
package com.impetus.client.couchbase.schemamanager;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.impetus.client.couchbase.CouchbaseBucketUtils;
import com.impetus.client.couchbase.CouchbaseConstants;
import com.impetus.client.couchbase.CouchbasePropertyReader;
import com.impetus.client.couchbase.CouchbasePropertyReader.CouchbaseSchemaMetadata;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class CouchbaseSchemaManager.
 * 
 * @author devender.yadav
 */
public class CouchbaseSchemaManager extends AbstractSchemaManager implements SchemaManager
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSchemaManager.class);

    /**
     * The Constant DEFAULT_RAM_SIZE_IN_MB.
     * 
     * Using 100 MB (minimum RAM Quota) be default
     */
    private static final int DEFAULT_RAM_SIZE_IN_MB = 100;

    /** The cluster. */
    private CouchbaseCluster cluster;

    /** The csmd. */
    private CouchbaseSchemaMetadata csmd = CouchbasePropertyReader.csmd;

    /** The cluster manager. */
    private ClusterManager clusterManager;

    /**
     * Instantiates a new couchbase schema manager.
     * 
     * @param clientFactory
     *            the client factory
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CouchbaseSchemaManager(String clientFactory, Map<String, Object> externalProperties,
            KunderaMetadata kunderaMetadata)
    {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.SchemaManager#dropSchema()
     */
    @Override
    public void dropSchema()
    {
        if (operation != null && ("create-drop").equalsIgnoreCase(operation))
        {
            for (TableInfo tableInfo : tableInfos)
            {
                removeBucket(tableInfo.getTableName());
            }
        }
        cluster.disconnect();
        cluster = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.SchemaManager#validateEntity
     * (java.lang.Class)
     */
    @Override
    public boolean validateEntity(Class clazz)
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * initiateClient ()
     */
    @Override
    protected boolean initiateClient()
    {
        for (String host : hosts)
        {
            if (host == null)
            {
                LOGGER.error("Host Name should not be null.");
                throw new IllegalArgumentException("Host Name should not be null.");
            }
        }
        cluster = CouchbaseCluster.create(hosts);

        if (userName != null && password != null)
        {
            clusterManager = cluster.clusterManager(userName, password);
        }
        else
        {
            clusterManager = cluster.clusterManager();
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#validate
     * (java.util.List)
     */
    @Override
    protected void validate(List<TableInfo> tableInfos)
    {

        if (!clusterManager.hasBucket(databaseName))
        {
            throw new SchemaGenerationException("Bucket [" + databaseName + "] does not exist.");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#update
     * (java.util.List)
     */
    @Override
    protected void update(List<TableInfo> tableInfos)
    {
        if (!clusterManager.hasBucket(databaseName))
        {
            addBucket(databaseName);
            createIdIndex(databaseName);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.schema.api.AbstractSchemaManager#create
     * (java.util.List)
     */
    @Override
    protected void create(List<TableInfo> tableInfos)
    {

        if (clusterManager.hasBucket(databaseName))
        {
            /*
             * Removing bucket will drop indexes too
             * 
             */
            removeBucket(databaseName);
        }
        addBucket(databaseName);
        createIdIndex(databaseName);
    }

    /**
     * Creates the id index.
     *
     * @param bucketName
     *            the bucket name
     */
    private void createIdIndex(String bucketName)
    {
        Bucket bucket = null;
        try
        {
            bucket = CouchbaseBucketUtils.openBucket(cluster, bucketName, csmd.getBucketProperty("bucket.password"));

            /*
             * Ignoring if indexes pre-exist
             */

            bucket.bucketManager().createN1qlPrimaryIndex(buildIndexName(bucketName), true, false);

            LOGGER.debug("Niql primary Index are created for bucket [" + bucketName + "].");
        }
        catch (CouchbaseException cex)
        {
            LOGGER.error("Not able to create Niql primary index for bucket [" + bucketName + "].", cex);
            throw new KunderaException("Not able to create Niql primary index for bucket [" + bucketName + "].", cex);
        }

        finally
        {
            CouchbaseBucketUtils.closeBucket(bucket);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * create_drop (java.util.List)
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfos)
    {
        create(tableInfos);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.schema.api.AbstractSchemaManager#
     * exportSchema(java.lang.String, java.util.List)
     */
    @Override
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas)
    {
        super.exportSchema(persistenceUnit, schemas);
    }

    /**
     * Removes the bucket.
     *
     * @param name
     *            the name
     */
    private void removeBucket(String name)
    {
        try
        {
            if (clusterManager.removeBucket(name))
            {
                LOGGER.info("Bucket [" + name + "] is removed!");
            }
            else
            {
                LOGGER.error("Not able to remove bucket [" + name + "].");
                throw new KunderaException("Not able to remove bucket [" + name + "].");
            }
        }
        catch (CouchbaseException ex)
        {
            LOGGER.error("Not able to remove bucket [" + name + "].", ex);
            throw new KunderaException("Not able to remove bucket [" + name + "].", ex);
        }
    }

    /**
     * Adds the bucket.
     *
     * @param name
     *            the name
     */
    private void addBucket(String name)
    {
        String qouta = csmd.getBucketProperty("bucket.quota");
        int bucketQuota = qouta != null ? Integer.parseInt(qouta) : DEFAULT_RAM_SIZE_IN_MB;
        BucketSettings bucketSettings = new DefaultBucketSettings.Builder().type(BucketType.COUCHBASE).name(name)
                .quota(bucketQuota).build();

        try
        {
            clusterManager.insertBucket(bucketSettings);
            LOGGER.info("Bucket [" + name + "] is added!");
        }
        catch (CouchbaseException ex)
        {
            LOGGER.error("Not able to add bucket [" + name + "].", ex);
            throw new KunderaException("Not able to add bucket [" + name + "].", ex);
        }
    }

    /**
     * Builds the index name.
     *
     * @param bucketName
     *            the bucket name
     * @return the string
     */
    private String buildIndexName(String bucketName)
    {
        if (bucketName == null)
        {
            throw new KunderaException("Bucket Name can't be null!");
        }
        return (bucketName + CouchbaseConstants.INDEX_SUFFIX).toLowerCase();
    }

}
