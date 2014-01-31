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

import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.loader.PersistenceLoaderException;
import com.impetus.kundera.loader.PersistenceXMLLoader;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * The Class PersistenceUnitConfiguration: 1) Find and load/configure
 * persistence unit metadata. Earlier it was PersistenceUnitLoader.
 * 
 * @author vivek.mishra
 */

public class PersistenceUnitConfiguration extends AbstractSchemaConfiguration implements Configuration
{

    /** The log instance. */
    private static Logger log = LoggerFactory.getLogger(PersistenceUnitConfiguration.class);

    /** The Constant PROVIDER_IMPLEMENTATION_NAME. */
    private static final String PROVIDER_IMPLEMENTATION_NAME = KunderaPersistence.class.getName();

    /**
     * Constructor parameterised with persistence units.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public PersistenceUnitConfiguration(Map properties, final KunderaMetadata metadata, String... persistenceUnits)
    {
        super(persistenceUnits, properties, metadata);
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

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        try
        {
            Map<String, PersistenceUnitMetadata> metadatas = findPersistenceMetadatas();
            for (String persistenceUnit : persistenceUnits)
            {
                if (!metadatas.containsKey(persistenceUnit))
                {
                    log.error("Unconfigured persistence unit: " + persistenceUnit
                            + " please validate with persistence.xml");
                    throw new PersistenceUnitConfigurationException("Invalid persistence unit: " + persistenceUnit
                            + " provided");
                }
            }
            log.info("Finishing persistence unit metadata configuration ...");
            appMetadata.addPersistenceUnitMetadata(metadatas);
        }
        catch (InvalidConfigurationException icex)
        {
            log.error("Error occurred during persistence unit configuration, Caused by: .", icex);
            throw new PersistenceLoaderException(icex);
        }
    }

    public void configure(PersistenceUnitInfo puInfo)
    {
        log.info("Loading Metadata from persistence.xml ...");

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        try
        {
            PersistenceUnitMetadata metadata = new PersistenceUnitMetadata(puInfo.getPersistenceXMLSchemaVersion(),
                    puInfo.getPersistenceUnitRootUrl(), null);

            metadata.setTransactionType(puInfo.getTransactionType());

            metadata.setClasses(puInfo.getManagedClassNames());

            metadata.setExcludeUnlistedClasses(puInfo.excludeUnlistedClasses());

            if (StringUtils.isBlank(puInfo.getPersistenceUnitName()))
            {
                throw new PersistenceUnitConfigurationException(
                        "Invalid persistence unit name, persistence unit name should not be null or blank.");
            }
            else
            {
                metadata.setPersistenceUnitName(puInfo.getPersistenceUnitName());
            }

            if (puInfo.getProperties().getProperty(PersistenceProperties.KUNDERA_CLIENT_FACTORY) != null)
            {
                log.error("kundera.client property is missing for persistence unit:" + puInfo.getPersistenceUnitName());
                throw new IllegalArgumentException("kundera.client property is missing for persistence unit:"
                        + puInfo.getPersistenceUnitName());
            }
            metadata.setProperties(puInfo.getProperties());

            if (puInfo.getPersistenceProviderClassName() == null
                    || PROVIDER_IMPLEMENTATION_NAME.equalsIgnoreCase(puInfo.getPersistenceProviderClassName()))
            {
                metadata.setProvider(puInfo.getPersistenceProviderClassName());
            }
            else
            {
                throw new PersistenceUnitConfigurationException("Invalid persistence provider : "
                        + puInfo.getPersistenceProviderClassName() + ", persistence provider must be "
                        + PROVIDER_IMPLEMENTATION_NAME + ".");
            }

            metadata.setPackages(puInfo.getMappingFileNames());

            for (URL url : puInfo.getJarFileUrls())
            {
                metadata.addJarFile(url.getPath());
            }

            Map<String, PersistenceUnitMetadata> metadatas = new HashMap<String, PersistenceUnitMetadata>();
            metadatas.put(puInfo.getPersistenceUnitName(), metadata);

            for (String persistenceUnit : persistenceUnits)
            {
                if (!metadatas.containsKey(persistenceUnit))
                {
                    log.error("Unconfigured persistence unit: " + persistenceUnit);
                    throw new PersistenceUnitConfigurationException("Invalid persistence unit: " + persistenceUnit
                            + " provided");
                }
            }
            log.info("Finishing persistence unit metadata configuration ...");
            appMetadata.addPersistenceUnitMetadata(metadatas);
        }
        catch (InvalidConfigurationException icex)
        {
            log.error("Error occurred during persistence unit configuration, Caused by: .", icex);
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
        String puLocation = (String) (externalPropertyMap != null
                && externalPropertyMap.get(Constants.PERSISTENCE_UNIT_LOCATIION) != null ? externalPropertyMap
                .get(Constants.PERSISTENCE_UNIT_LOCATIION) : Constants.DEFAULT_PERSISTENCE_UNIT_LOCATIION);

        Enumeration<URL> xmls = null;
        try
        {
            xmls = this.getClass().getClassLoader().getResources(puLocation);
        }
        catch (IOException ioex)
        {
            log.warn("Error while loading persistence.xml, Caused by: {}.", ioex);
        }

        if (xmls == null || !xmls.hasMoreElements())
        {
            log.error("Could not find any META-INF/persistence.xml file in the classpath");
            throw new InvalidConfigurationException("Could not find any META-INF/persistence.xml file in the classpath");
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
                    if (log.isWarnEnabled())
                    {
                        log.warn("Duplicate persistence-units for name: " + metadata.getPersistenceUnitName()
                                + ". verify your persistence.xml file");
                    }
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
