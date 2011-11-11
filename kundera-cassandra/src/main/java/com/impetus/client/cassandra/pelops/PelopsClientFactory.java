package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;

import com.impetus.client.cassandra.index.SolandraUtils;
import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class PelopsClientFactory extends GenericClientFactory
{
    private static Logger logger = Logger.getLogger(KunderaPersistence.class);

    IndexManager indexManager;

    @Override
    protected void initializeClient()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        String serverConfig = (String) persistenceUnitMetadata.getProperties().get("server.config");

        if (serverConfig != null)
        {
            serverConfig = "file:///" + serverConfig;
            System.setProperty("cassandra.config", serverConfig);
        }

        // Start Solandra specific tasks
        // initializeSolandra(getPersistenceUnit());

        // TODO StandardAnalyzer is thread safe. So it looks like indexManager
        // is threadsafe an hence using a single instance
        logger.info("Initializing Threadsafe Indexmanager. Is it really threadsafe?");
        indexManager = new IndexManager(new LuceneIndexer(new KeywordAnalyzer())/*
                                                                                 * new
                                                                                 * SolandraIndexer
                                                                                 * (
                                                                                 * new
                                                                                 * StandardAnalyzer
                                                                                 * (
                                                                                 * Version
                                                                                 * .
                                                                                 * LUCENE_CURRENT
                                                                                 * )
                                                                                 * )
                                                                                 */);
    }

    private void initializeSolandra(String persistenceUnit)
    {
        logger.info("Initializing Solandra.");
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
        String node = puMetadata.getProperties().getProperty("kundera.nodes");
        String port = puMetadata.getProperties().getProperty("kundera.port");
        new SolandraUtils().initializeSolandra(node, Integer.valueOf(port));
    }

    @Override
    protected Object createPoolOrConnection()
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(getPersistenceUnit());

        Properties props = persistenceUnitMetadata.getProperties();
        String contactNodes = (String) props.get("kundera.nodes");
        String defaultPort = (String) props.get("kundera.port");
        String keyspace = (String) props.get("kundera.keyspace");
        String poolName = PelopsUtils.generatePoolName(getPersistenceUnit());
        if (Pelops.getDbConnPool(poolName) == null)
        {
            Cluster cluster = new Cluster(contactNodes,
                    new IConnection.Config(Integer.parseInt(defaultPort), true, -1), false);
            Pelops.addPool(poolName, cluster, keyspace);
        }
        // TODO return a thrift pool
        return null;
    }

    @Override
    protected Client instantiateClient()
    {
        return new PelopsClient(indexManager);
    }

    @Override
    protected boolean isClientThreadSafe()
    {
        return false;
    }

    @Override
    public void unload(String... persistenceUnits)
    {
        Pelops.shutdown();
    }

}
