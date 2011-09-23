/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.ejb;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.startup.model.KunderaMetadata;
import com.impetus.kundera.startup.model.PersistenceUnitMetadata;

/**
 * Builds EmtityManagerFactory instances from classpath.
 * 
 * @author animesh.kumar
 */
public class EntityManagerFactoryBuilder
{

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(EntityManagerFactoryBuilder.class);

    /** The Constant PROVIDER_IMPLEMENTATION_NAME. */
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();

    /**
     * Builds up EntityManagerFactory for a given persistenceUnitName and
     * overriding properties.
     * 
     * @param persistenceUnitName
     *            the persistence unit name
     * @param override
     *            the override
     * @return the entity manager factory
     */
    public EntityManagerFactory buildEntityManagerFactory(String persistenceUnitName, Map<Object, Object> override)
    {
        Properties props = new Properties();
        // Override properties
        
        KunderaMetadata kunderaMetadata = KunderaMetadata.getInstance();        
        PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnitName);
        
        Properties metadataProperties = puMetadata.getProperties();
        // Make sure, it's empty or Unmodifiable
        override = override == null ? Collections.EMPTY_MAP : Collections.unmodifiableMap(override);

        // Take all from Metadata and override with supplied map
        for (Map.Entry<Object, Object> entry : metadataProperties.entrySet())
        {
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (override.containsKey(key))
            {
                value = override.get(key);
            }
            props.put(key, value);
        }

        // Now take all the remaining ones from override
        for (Map.Entry<Object, Object> entry : override.entrySet())
        {
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (!props.containsKey(key))
            {
                props.put(key, value);
            }
        }

        LOG.info("Building EntityManagerFactory for name: " + puMetadata.getPersistenceUnitName() + ", and Properties:" + props);
        return new EntityManagerFactoryImpl(puMetadata, props);
    }

}
