package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class PelopsUtils
{
    private static Logger logger = LoggerFactory.getLogger(PelopsUtils.class);

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

    public static Policy getPoolConfigPolicy(PersistenceUnitMetadata persistenceUnitMetadata)
    {
        Policy policy = new Policy();

        Properties props = persistenceUnitMetadata.getProperties();

        String maxActivePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
        String maxIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_IDLE);
        String minIdlePerNode = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MIN_IDLE);
        String maxTotal = props.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_TOTAL);

        try
        {
            if (!StringUtils.isEmpty(maxActivePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxActivePerNode));
            }

            if (!StringUtils.isEmpty(maxIdlePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxIdlePerNode));
            }

            if (!StringUtils.isEmpty(minIdlePerNode))
            {
                policy.setMaxActivePerNode(Integer.parseInt(minIdlePerNode));
            }

            if (!StringUtils.isEmpty(maxTotal))
            {
                policy.setMaxActivePerNode(Integer.parseInt(maxTotal));
            }
        }
        catch (NumberFormatException e)
        {
            logger.warn("Some Connection pool related property for " + persistenceUnitMetadata.getPersistenceUnitName()
                    + " persistence unit couldn't be parsed. Default pool policy would be used");
            policy = null;
        }
        return policy;
    }

}
