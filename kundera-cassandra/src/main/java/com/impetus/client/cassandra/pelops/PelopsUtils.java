package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class PelopsUtils
{
    public static String generatePoolName(String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadatata.getProperties();
        String contactNodes = (String) props.get("kundera.nodes");
        String defaultPort = (String) props.get("kundera.port");
        String keyspace = (String) props.get("kundera.keyspace");

        return contactNodes + ":" + defaultPort + ":" + keyspace;
    }

}
