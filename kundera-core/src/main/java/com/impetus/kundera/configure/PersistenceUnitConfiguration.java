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
package com.impetus.kundera.configure;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.spi.PersistenceUnitTransactionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.loader.PersistenceLoaderException;
import com.impetus.kundera.loader.PersistenceXMLLoader;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * The Class PersistenceUnitLoader: 1) Find and load/configure persistence unit
 * meta data.
 * 
 * @author vivek.mishra
 */

class PersistenceUnitConfiguration implements Configuration
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(PersistenceUnitConfiguration.class);

    /** The Constant PROVIDER_IMPLEMENTATION_NAME. */
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();

    /** Holding instance for persistence units. */
    private String[] persistenceUnits;

    
    /**
     * Constructor parameterised with persistence units. 
     * @param persistenceUnits persistence units.
     */
    PersistenceUnitConfiguration(String...persistenceUnits)
    {
     this.persistenceUnits = persistenceUnits;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.Configuration#configure()
     */
    @Override
    public void configure()
    {
        log.debug("Loading Metadata from persistence.xml...");
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        List<PersistenceUnitMetadata> metadatas;
        try
        {
            metadatas = findPersistenceMetadatas();
        }
        catch (InvalidConfigurationException e)
        {
            throw new PersistenceLoaderException(e);
        }

        for (String persistenceUnit : persistenceUnits)
        {
            if (appMetadata.getPersistenceUnitMetadataMap().get(persistenceUnit.trim()) != null)
            {
                log.debug("Metadata already exists for the Persistence Unit " + persistenceUnit + ". Nothing to do");

            }
            else
            {
                appMetadata.addPersistenceUnitMetadata(persistenceUnit,
                        getPersistenceMetadata(metadatas, persistenceUnit));
            }
        }
    }

    /**
     * Configure and load the persistence metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit name
     * @return the persistence metadata
     */
    private PersistenceUnitMetadata getPersistenceMetadata(List<PersistenceUnitMetadata> metadatas,
            String persistenceUnit)
    {
        log.info("Looking up for persistence unit: " + persistenceUnit);

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
            throw new IllegalArgumentException("No name provided and several persistence units found. "
                    + "Check whether you correctly provided persistence unit name");
        }

        // Look for one that interests us
        for (PersistenceUnitMetadata metadata : metadatas)
        {
            if (metadata.getPersistenceUnitName().equals(persistenceUnit))
            {
                return metadata;
            }
        }

        throw new PersistenceLoaderException("Could not find persistence unit in the classpath for name: "
                + persistenceUnit);
    }

    /**
     * Find persistence meta data. Loads configured persistence.xml and load all
     * provided configurations within persistence meta data as per @see JPA 2.0
     * specifications.
     * 
     * @return the list configure persistence unit meta data.
     */
    private List<PersistenceUnitMetadata> findPersistenceMetadatas() throws InvalidConfigurationException
    {

        Enumeration<URL> xmls = null;
        try
        {
            xmls = Thread.currentThread().getContextClassLoader().getResources("META-INF/persistence.xml");
        }
        catch (IOException ioex)
        {
            log.warn("Error while loading persistence.xml caused by:" + ioex.getMessage());
        }

        if (xmls == null || !xmls.hasMoreElements())
        {
            log.info("Could not find any META-INF/persistence.xml " + " file in the classpath");
            throw new InvalidConfigurationException("Could not find any META-INF/persistence.xml "
                    + " file in the classpath");
        }

        Set<String> persistenceUnitNames = new HashSet<String>();
        List<PersistenceUnitMetadata> persistenceUnits = new ArrayList<PersistenceUnitMetadata>();

        while (xmls.hasMoreElements())
        {
            URL url = xmls.nextElement();

            log.trace("Analysing persistence.xml: " + url);
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
                    throw new InvalidConfigurationException("Duplicate persistence-units for name: "
                            + metadata.getPersistenceUnitName() + ". verify your persistence.xml file");
                }
                persistenceUnitNames.add(metadata.getPersistenceUnitName());
            }
        }

        return persistenceUnits;

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
