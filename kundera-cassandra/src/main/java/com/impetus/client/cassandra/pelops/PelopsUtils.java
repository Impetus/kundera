/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.cassandra.pelops;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class PelopsUtils.
 */
public class PelopsUtils
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(PelopsUtils.class);

    /**
     * Generate pool name.
     *
     * @param persistenceUnit the persistence unit
     * @return the string
     */
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

    /**
     * Gets the pool config policy.
     *
     * @param persistenceUnitMetadata the persistence unit metadata
     * @return the pool config policy
     */
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
