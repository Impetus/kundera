package com.impetus.client.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientLoader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class HBaseClientLoader extends GenericClientLoader
{
    IndexManager indexManager;

    @Override
    protected void initialize()
    {
        indexManager = new IndexManager(new LuceneIndexer(new StandardAnalyzer(Version.LUCENE_CURRENT)));
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
        return false;
    }

    @Override
    public void unload(String persistenceUnit)
    {
        // TODO destroy pool
    }

}
