package com.impetus.client.couchbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.impetus.kundera.KunderaException;

/**
 * The Class CouchbaseBucketUtils.
 * 
 * @author devender.yadav
 * 
 */
public class CouchbaseBucketUtils
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseBucketUtils.class);

    /**
     * Instantiates a new couchbase bucket utils.
     */
    private CouchbaseBucketUtils()
    {
    }

    /**
     * Open bucket.
     *
     * @param cluster
     *            the cluster
     * @param name
     *            the name
     * @return the bucket
     */
    public static Bucket openBucket(CouchbaseCluster cluster, String name)
    {
        clusterNullCheck(cluster);
        try
        {
            Bucket bucket = cluster.openBucket(name);
            LOGGER.debug("Bucket [" + name + "] is opened!");
            return bucket;
        }
        catch (CouchbaseException ex)
        {
            LOGGER.error("Not able to open bucket [" + name + "].", ex);
            throw new KunderaException("Not able to open bucket [" + name + "].", ex);
        }

    }

    /**
     * Close bucket.
     *
     * @param cluster
     *            the cluster
     * @param bucket
     *            the bucket
     */
    public static void closeBucket(CouchbaseCluster cluster, Bucket bucket)
    {
        if (bucket != null)
        {
            clusterNullCheck(cluster);
            if (!bucket.close())
            {
                LOGGER.error("Not able to close bucket [" + bucket.name() + "].");
                throw new KunderaException("Not able to close bucket [" + bucket.name() + "].");
            }
            else
            {
                LOGGER.debug("Bucket [" + bucket.name() + "] is closed!");
            }
        }
    }

    /**
     * Cluster null check.
     *
     * @param cluster
     *            the cluster
     */
    private static void clusterNullCheck(CouchbaseCluster cluster)
    {
        if (cluster == null)
        {
            LOGGER.error("CouchbaseCluster object can't be null");
            throw new IllegalArgumentException("CouchbaseCluster object can't be null");
        }
    }

}
