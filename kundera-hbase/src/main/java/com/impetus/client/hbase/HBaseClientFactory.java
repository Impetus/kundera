package com.impetus.client.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

public class HBaseClientFactory extends GenericClientFactory
{
    private IndexManager indexManager;

    private HBaseConfiguration conf;

    private HTablePool hTablePool;

    private EntityReader reader;

    private static final int DEFAULT_POOL_SIZE = 100;

    int poolSize;

    @Override
    protected void initializeClient()
    {
        // Initialize Index Manager
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34)));

        // Initialize HBase configuration
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(getPersistenceUnit());

        String node = puMetadata.getProperties().getProperty("kundera.nodes");
        String port = puMetadata.getProperties().getProperty("kundera.port");
        String poolSize = puMetadata.getProperties().getProperty("kundera.pool.size");

        if (StringUtils.isEmpty(poolSize))
        {
            this.poolSize = DEFAULT_POOL_SIZE;
        }
        else
        {
            this.poolSize = Integer.parseInt(poolSize);
        }

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", node + ":" + port);
        conf = new HBaseConfiguration(hadoopConf);
        reader = new HBaseEntityReader();
    }

    @Override
    protected Object createPoolOrConnection()
    {
        // TODO: Make pool size configurable, extract from persistence.xml
        hTablePool = new HTablePool(conf, poolSize);
        return hTablePool;
    }

    @Override
    protected Client instantiateClient()
    {
        return new HBaseClient(indexManager, conf, hTablePool, reader);
    }

    @Override
    protected boolean isClientThreadSafe()
    {
        return true;
    }

    @Override
    public void unload(String... persistenceUnits)
    {
        // TODO destroy pool
        // hTablePool = null;
    }

}
