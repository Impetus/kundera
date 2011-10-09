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
package com.impetus.kundera;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.loader.ApplicationLoader;
import com.impetus.kundera.loader.CoreLoader;
import com.impetus.kundera.persistence.EntityManagerFactoryBuilder;

/**
 * The Class KunderaPersistence.
 * 
 * @author animesh.kumar
 */
@SuppressWarnings("unchecked")
public class KunderaPersistence implements PersistenceProvider
{

    /** The logger. */
    private static Logger logger = Logger.getLogger(KunderaPersistence.class);

    /** The emf map. */
    private static Map<String, EntityManagerFactory> emfMap = new HashMap<String, EntityManagerFactory>();

    /**
     * Instantiates a new kundera persistence.
     */
    public KunderaPersistence()
    {
        // Load Core
        logger.info("Loading Core");
        new CoreLoader().load();
    }

    @Override
    public final EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map)
    {
        return createEntityManagerFactory(info.getPersistenceUnitName(), map);
    }

    @Override
    public final EntityManagerFactory createEntityManagerFactory(String persistenceUnit, Map map)
    {
        // TODO Pooling of factories. Current code caches a single factory and
        // uses it every time.
        if (emfMap.get(persistenceUnit) != null)
        {
            logger.info("Returning existing factory for persistence unit : " + persistenceUnit);
            return emfMap.get(persistenceUnit);
        }
        else
        {

            logger.info("Creating non-existing factory for persistence unit : " + persistenceUnit);

            // Initialize Persistence Unit Related Parts
            initializeKundera(persistenceUnit);

            logger.info("Preparing EntityManagerFactoryBuilder for persistence unit : " + persistenceUnit);
            EntityManagerFactoryBuilder builder = new EntityManagerFactoryBuilder();
            return builder.buildEntityManagerFactory(persistenceUnit, map);

        }
    }

    private void initializeKundera(String persistenceUnit)
    {
        // Invoke Application MetaData
        logger.info("Loading Application MetaData For Persistence Unit " + persistenceUnit);
        (new ApplicationLoader()).load(persistenceUnit);

        // Invoke Client Loaders
        logger.info("Loading Client For Persistence Unit " + persistenceUnit);
        ClientResolver.getClientLoader(persistenceUnit).load(persistenceUnit);
    }

    @Override
    public ProviderUtil getProviderUtil()
    {
        throw new NotImplementedException("TODO");
    }

}
