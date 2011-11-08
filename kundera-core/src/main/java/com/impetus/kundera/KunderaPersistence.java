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
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryBuilder;
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
    private static Logger logger = Logger.getLogger(KunderaPersistence.class);

    /** Instance of Entity Manager Factory */
    private static EntityManagerFactory emf;   
    
    
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
        //TODO: Pooling of factories. Current code caches a single factory and uses it every time.
        if (emf != null)
        {
            logger.info("Returning existing factory " + emf);
            return emf;
        } else {
            logger.info("Creating non-existing factory for persistence unit(s) : " + persistenceUnit);
            EntityManagerFactoryBuilder builder = new EntityManagerFactoryBuilder();
            //emf = builder.buildEntityManagerFactory(persistenceUnit, map); 
            emf = new EntityManagerFactoryImpl(persistenceUnit, new HashMap<String, Object>());
        }
        
        //One time initialization (Application and Client level)
        initializeKundera(persistenceUnit);

        
        return emf;

        
    }
    
    /**
     * One time initialization at Application and Client level
     * @param persistenceUnit Persistence Unit/ Comma separated persistence units
     */
    private void initializeKundera(String persistenceUnit)
    {
        // Invoke Application MetaData
        logger.info("Loading Application MetaData and Initializing Client(s) For Persistence Unit(s) " 
                + persistenceUnit);
        
        String[] persistenceUnits = persistenceUnit.split(Constants.PERSISTENCE_UNIT_SEPARATOR);
        
        
        (new ApplicationLoader()).load(persistenceUnits);

        // Invoke Client Loaders
        logger.info("Loading Client(s) For Persistence Unit(s) " + persistenceUnit);
        for(String pu : persistenceUnits) {
            ClientResolver.getClientFactory(pu).load(pu);
        }      
        
    }

    @Override
    public ProviderUtil getProviderUtil()
    {
        throw new NotImplementedException("TODO");
    }

}
