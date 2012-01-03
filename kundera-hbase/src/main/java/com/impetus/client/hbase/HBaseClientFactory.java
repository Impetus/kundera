package com.impetus.client.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class HBaseClientFactory extends GenericClientFactory
{
    IndexManager indexManager;

    @Override
    protected void initializeClient()
    {
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34)));
    }

    @Override
    protected Object createPoolOrConnection()
    {
        return null;
    }

    @Override
    protected Client instantiateClient()
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(getPersistenceUnit());
        String node = puMetadata.getProperties().getProperty("kundera.nodes");
        String port = puMetadata.getProperties().getProperty("kundera.port");

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", node + ":" + port);
        HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
        return new HBaseClient(indexManager, conf);
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
    }

}
