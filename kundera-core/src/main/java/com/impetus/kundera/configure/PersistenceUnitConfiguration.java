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
package com.impetus.kundera.configure;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * The Class PersistenceUnitConfiguration: 1) Find and load/configure
 * persistence unit meta data. Earlier it was PersistenceUnitLoader.
 * 
 * @author vivek.mishra
 */

public class PersistenceUnitConfiguration implements Configuration
{

    /** The log instance. */
    private static Logger log = LoggerFactory.getLogger(PersistenceUnitConfiguration.class);

    /** The Constant PROVIDER_IMPLEMENTATION_NAME. */
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();

    /** Holding instance for persistence units. */
    protected String[] persistenceUnits;

    /**
     * Constructor parameterised with persistence units.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public PersistenceUnitConfiguration(String... persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.Configuration#configure()
     */
    @Override
    public void configure()
    {
        log.info("Loading Metadata from persistence.xml ...");
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        Map<String, PersistenceUnitMetadata> metadatas;
        try
        {
            metadatas = findPersistenceMetadatas();
            for (String persistenceUnit : persistenceUnits)
            {
                if (!metadatas.containsKey(persistenceUnit))
                {
                    log.error("Unconfigured persistence unit: " + persistenceUnit
                            + " please validate with persistence.xml");
                    throw new PersistenceUnitConfigurationException("Invalid persistence unit: " + persistenceUnit + " provided");
                }
//                metadatas.get(persistenceUnit);
            }
            log.info("Finishing persistence unit metadata configuration ...");
            appMetadata.addPersistenceUnitMetadata(metadatas);
        }
        catch (InvalidConfigurationException icex)
        {
            log.error("Error occurred during persistence unit configuration, Caused by:" + icex.getMessage());
            throw new PersistenceLoaderException(icex);
        }

    }

    /**
     * Find persistence meta data. Loads configured persistence.xml and load all
     * provided configurations within persistence meta data as per @see JPA 2.0
     * specifications.
     * 
     * @return the list configure persistence unit meta data.
     */
    private Map<String, PersistenceUnitMetadata> findPersistenceMetadatas() throws InvalidConfigurationException
    {

        Enumeration<URL> xmls = null;
        try
        {
            xmls = Thread.currentThread().getContextClassLoader().getResources("META-INF/persistence.xml");
        }
        catch (IOException ioex)
        {
            log.warn("Error while loading persistence.xml Caused by:" + ioex.getMessage());
        }

        if (xmls == null || !xmls.hasMoreElements())
        {
            log.info("Could not find any META-INF/persistence.xml " + " file in the classpath");
            throw new InvalidConfigurationException("Could not find any META-INF/persistence.xml "
                    + " file in the classpath");
        }

        Set<String> persistenceUnitNames = new HashSet<String>();
        Map<String, PersistenceUnitMetadata> persistenceUnitMap = new HashMap<String, PersistenceUnitMetadata>();
        while (xmls.hasMoreElements())
        {
            URL url = xmls.nextElement();

            log.trace("Analysing persistence.xml: " + url);
            List<PersistenceUnitMetadata> metadataFiles = PersistenceXMLLoader.findPersistenceUnits(url,
                    PersistenceUnitTransactionType.RESOURCE_LOCAL);

            // Pick only those that have Kundera Provider
            for (PersistenceUnitMetadata metadata : metadataFiles)
            {
                // check for unique names
                if (persistenceUnitNames.contains(metadata.getPersistenceUnitName()))
                {
                    throw new InvalidConfigurationException("Duplicate persistence-units for name: "
                            + metadata.getPersistenceUnitName() + ". verify your persistence.xml file");
                }

                // check for provider
                if (metadata.getPersistenceProviderClassName() == null
                        || PROVIDER_IMPLEMENTATION_NAME.equalsIgnoreCase(metadata.getPersistenceProviderClassName()))
                {
                    persistenceUnitMap.put(metadata.getPersistenceUnitName(), metadata);
                }

                // add to check for duplicate persistence unit.
                persistenceUnitNames.add(metadata.getPersistenceUnitName());
            }
        }
        return persistenceUnitMap;
    }
}
