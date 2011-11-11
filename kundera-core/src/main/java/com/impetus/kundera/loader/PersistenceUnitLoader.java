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
package com.impetus.kundera.loader;

import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.log4j.Logger;

import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author amresh.singh
 * 
 */
public class PersistenceUnitLoader extends ApplicationLoader
{
    private static Logger log = Logger.getLogger(PersistenceUnitLoader.class);

    /** The Constant PROVIDER_IMPLEMENTATION_NAME. */
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();

    @Override
    public void load(String... persistenceUnits)
    {
        log.debug("Loading Metadata from persistence.xml...");
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        for (String persistenceUnit : persistenceUnits)
        {
            if (appMetadata.getPersistenceUnitMetadataMap().get(persistenceUnit) != null)
            {
                log.debug("Metadata already exists for the Persistence Unit " + persistenceUnit + ". Nothing to do");

            }
            else
            {
                appMetadata.addPersistenceUnitMetadata(persistenceUnit, getPersistenceMetadata(persistenceUnit));
            }
        }
    }

    /**
     * Gets the persistence metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit name
     * @return the persistence metadata
     */
    private PersistenceUnitMetadata getPersistenceMetadata(String persistenceUnit)
    {
        log.info("Looking up for persistence unit: " + persistenceUnit);
        List<PersistenceUnitMetadata> metadatas = findPersistenceMetadatas();

        // If there is just ONE persistenceUnit, then use this irrespective of
        // the name
        if (metadatas.size() == 1)
        {
            return metadatas.get(0);
        }

        // Since there is more persistenceUnits, you must provide a name to look
        // up
        if (isEmpty(persistenceUnit))
        {
            throw new PersistenceException("No name provided and several persistence units found");
        }

        // Look for one that interests us
        for (PersistenceUnitMetadata metadata : metadatas)
        {
            if (metadata.getPersistenceUnitName().equals(persistenceUnit))
            {
                return metadata;
            }
        }

        throw new PersistenceException("Could not find persistence unit in the classpath for name: " + persistenceUnit);
    }

    /**
     * Find persistence metadatas.
     * 
     * @return the list
     */
    private List<PersistenceUnitMetadata> findPersistenceMetadatas()
    {
        try
        {
            Enumeration<URL> xmls = Thread.currentThread().getContextClassLoader()
                    .getResources("META-INF/persistence.xml");

            if (!xmls.hasMoreElements())
            {
                log.info("Could not find any META-INF/persistence.xml " + "file in the classpath");
            }

            Set<String> persistenceUnitNames = new HashSet<String>();
            List<PersistenceUnitMetadata> persistenceUnits = new ArrayList<PersistenceUnitMetadata>();

            while (xmls.hasMoreElements())
            {
                URL url = xmls.nextElement();
                log.trace("Analyse of persistence.xml: " + url);
                List<PersistenceUnitMetadata> metadataFiles = PersistenceXMLLoader.findPersistenceUnits(url,
                        PersistenceUnitTransactionType.RESOURCE_LOCAL);

                // Pick only those that have Kundera Provider
                for (PersistenceUnitMetadata metadata : metadataFiles)
                {
                    // check for provider
                    if (metadata.getProvider() == null
                            || PROVIDER_IMPLEMENTATION_NAME.equalsIgnoreCase(metadata.getProvider()))
                    {
                        persistenceUnits.add(metadata);
                    }

                    // check for unique names
                    if (persistenceUnitNames.contains(metadata.getPersistenceUnitName()))
                    {
                        throw new PersistenceException("Duplicate persistence-units for name: "
                                + metadata.getPersistenceUnitName());
                    }
                    persistenceUnitNames.add(metadata.getPersistenceUnitName());
                }
            }

            return persistenceUnits;
        }
        catch (Exception e)
        {
            if (e instanceof PersistenceException)
            {
                throw (PersistenceException) e;
            }
            else
            {
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Checks if is empty.
     * 
     * @param str
     *            the str
     * @return true, if is empty
     */
    private static boolean isEmpty(String str)
    {
        return null == str || str.isEmpty();
    }

}
