package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.query.CassandraEntityReader;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

public class PelopsClientFactory extends GenericClientFactory
{
    private static Logger logger = LoggerFactory.getLogger(PelopsClientFactory.class);

    IndexManager indexManager;

    private EntityReader reader;

    @Override
    protected void initializeClient()
    {
        // TODO StandardAnalyzer is thread safe. So it looks like indexManager
        // is threadsafe an hence using a single instance
        logger.info("Initializing Threadsafe Indexmanager. Is it really threadsafe?");
        // indexManager = new IndexManager(new LuceneIndexer(new
        // KeywordAnalyzer()/*new StandardAnalyzer(Version.LUCENE_34*/)/*
        // * new
        // * KeywordAnalyzer
        // * (
        // * )
        // * )
        // *//*)*/);

        String luceneDirPath = MetadataUtils.getLuceneDirectory(getPersistenceUnit());
        indexManager = new IndexManager(LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34),
                luceneDirPath));

        reader = new CassandraEntityReader();

    }

    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit());

        if (Pelops.getDbConnPool(poolName) == null)
        {
            Cluster cluster = new Cluster(contactNodes,
                    new IConnection.Config(Integer.parseInt(defaultPort), true, -1), false);

            Policy policy = PelopsUtils.getPoolConfigPolicy(persistenceUnitMetadata);

            // Add pool with specified policy. null means default operand
            // policy.
            Pelops.addPool(poolName, cluster, keyspace, policy, null);

        }
        // TODO return a thrift pool
        return null;
    }

    @Override
    protected Client instantiateClient()
    {
        return new PelopsClient(indexManager, reader);
    }

    @Override
    protected boolean isClientThreadSafe()
    {
        return false;
    }

    @Override
    public void unload(String... persistenceUnits)
    {
        indexManager.close();
        // Pelops.shutdown();
    }

}
