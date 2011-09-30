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
import com.impetus.kundera.client.ClientType;
import com.impetus.kundera.loader.ApplicationLoader;
import com.impetus.kundera.loader.CoreLoader;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
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
        logger.debug("Loading Core");
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
            logger.debug("Returning existing factory for persistence unit : " + persistenceUnit);
            return emfMap.get(persistenceUnit);
        }
        else
        {

            logger.debug("Creating non-existing factory for persistence unit : " + persistenceUnit);

            // Initialize Persistence Unit Related Parts
            initializeKundera(persistenceUnit);

            EntityManagerFactoryBuilder builder = new EntityManagerFactoryBuilder();
            return builder.buildEntityManagerFactory(persistenceUnit, map);

        }
    }

    private void initializeKundera(String persistenceUnit)
    {
        // Invoke Application MetaData
        logger.debug("Loading Application MetaData For Persistence Unit " + persistenceUnit);
        (new ApplicationLoader()).load(persistenceUnit);

        // Invoke Client Loaders
        logger.debug("Loading Client MetaData For Persistence Unit " + persistenceUnit);
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.getInstance().getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String kunderaClientName = (String) persistenceUnitMetadata.getProperties().get("kundera.client");
        ClientType clientType = ClientType.getValue(kunderaClientName.toUpperCase());
        ClientResolver.getClientLoader(clientType).load(persistenceUnit);
    }

    @Override
    public ProviderUtil getProviderUtil()
    {
        throw new NotImplementedException("TODO");
    }

}
