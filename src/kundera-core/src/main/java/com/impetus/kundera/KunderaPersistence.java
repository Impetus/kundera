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
package com.impetus.kundera;

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.Configurator;
import com.impetus.kundera.configure.PersistenceUnitConfigurationException;
import com.impetus.kundera.loader.CoreLoader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * The Class KunderaPersistence.
 * 
 * @author animesh.kumar
 */
@SuppressWarnings("unchecked")
public class KunderaPersistence implements PersistenceProvider
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaPersistence.class);

    private final ProviderUtil providerUtil;

    private final PersistenceUtilHelper.MetadataCache cache;

    /**
     * Instantiates a new kundera persistence.
     */
    public KunderaPersistence()
    {
        // Load Core
        logger.info("Loading Core");
        new CoreLoader().load();

        this.providerUtil = new KunderaPersistenceProviderUtil(this);
        this.cache = new PersistenceUtilHelper.MetadataCache();
    }

    @Override
    public final EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map)
    {
        return createEntityManagerFactory(info.getPersistenceUnitName(), map);
    }

    @Override
    public synchronized final EntityManagerFactory createEntityManagerFactory(String persistenceUnit, Map map)
    {
        // TODO: This may be a comma separated PU list, synchronizing on this
        // list may not be intended
        if (persistenceUnit == null)
        {
            throw new KunderaException("Persistence unit should not be null");
        }
        synchronized (persistenceUnit)
        {
            try
            {
                initializeKundera(persistenceUnit, map);

                EntityManagerFactory emf = new EntityManagerFactoryImpl(persistenceUnit, map);

                return emf;
            }
            catch (PersistenceUnitConfigurationException pcex)
            {
                // Means it is not for kundera persistence!
                return null;
            }
        }
    }

    /**
     * One time initialization at Application and Client level.
     * 
     * @param persistenceUnit
     *            Persistence Unit/ Comma separated persistence units
     */
    private void initializeKundera(String persistenceUnit, Map props)
    {
        // Invoke Application MetaData
        logger.info("Loading Application MetaData and Initializing Client(s) For Persistence Unit(s) "
                + persistenceUnit);

        String[] persistenceUnits = persistenceUnit.split(Constants.PERSISTENCE_UNIT_SEPARATOR);

        new Configurator(props, persistenceUnits).configure();

    }

    /**
     * Returns Persistence Provider util
     * 
     * @see javax.persistence.spi.PersistenceProvider#getProviderUtil()
     */
    @Override
    public ProviderUtil getProviderUtil()
    {
        return this.providerUtil;
    }

    /**
     * @return the cache
     */
    public PersistenceUtilHelper.MetadataCache getCache()
    {
        return cache;
    }

}
