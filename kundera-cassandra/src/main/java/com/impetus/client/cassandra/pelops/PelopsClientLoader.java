package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Pelops;

import com.impetus.client.cassandra.index.SolandraUtils;
import com.impetus.kundera.loader.Loader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class PelopsClientLoader implements Loader
{

    @Override
    public void load(String persistenceUnit)
    {
    	
        loadClientMetadata(persistenceUnit);

        setServerConfigAsSystemProperty(persistenceUnit);

        // Start Solandra specific tasks
        initializeSolandra(persistenceUnit);
        
        //Initialize Pelops Pool
        initializePelopsPool(persistenceUnit);

    }

    private void loadClientMetadata(String persistenceUnit)
    {
        ClientMetadata clientMetadata = new ClientMetadata();

        // TODO Make a client properties file
        clientMetadata.setClientImplementor("com.impetus.client.cassandra.pelops.PelopsClient");
        clientMetadata.setIndexImplementor("com.impetus.client.cassandra.index.SolandraIndexer");
        
        if (KunderaMetadata.getInstance().getClientMetadata(persistenceUnit) == null)
            KunderaMetadata.getInstance().addClientMetadata(persistenceUnit, clientMetadata);
    }

    private void setServerConfigAsSystemProperty(String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.getInstance().getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);

        String serverConfig = (String) persistenceUnitMetadata.getProperties().get("server.config");

        if (serverConfig != null)
        {
            serverConfig = "file:///" + serverConfig;
            System.setProperty("cassandra.config", serverConfig);
        }
    }

    /**
     * @param persistenceUnit
     */
    private void initializeSolandra(String persistenceUnit)
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
        String node = puMetadata.getProperties().getProperty("kundera.nodes");
        String port = puMetadata.getProperties().getProperty("kundera.port");
        new SolandraUtils().initializeSolandra(node, Integer.valueOf(port));
    }
    
    /**
     * Configure pool.
     * 
     * @param keyspace
     *            the keyspace
     */
    private void initializePelopsPool(String persistenceUnit)
    {        
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.getInstance().getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
        
        Properties props = persistenceUnitMetadata.getProperties();
        String contactNodes = (String) props.get("kundera.nodes");
        String defaultPort = (String) props.get("kundera.port");
        String keyspace = (String) props.get("kundera.keyspace");
        
        
    	Cluster cluster = new Cluster(contactNodes, new IConnection.Config(Integer.parseInt(defaultPort), true, -1), false);    
    	String poolName = contactNodes + ":" + defaultPort +  ":" + keyspace;
        Pelops.addPool(poolName , cluster, keyspace);         

    }

}
