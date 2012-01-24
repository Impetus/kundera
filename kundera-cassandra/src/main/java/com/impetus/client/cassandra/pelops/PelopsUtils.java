package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class PelopsUtils
{
    public static String generatePoolName(String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadatata.getProperties();
        String contactNodes = (String) props.get(PersistenceProperties.KUNDERA_NODES);
        String defaultPort = (String) props.get(PersistenceProperties.KUNDERA_PORT);
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);

        return contactNodes + ":" + defaultPort + ":" + keyspace;
    }

}
