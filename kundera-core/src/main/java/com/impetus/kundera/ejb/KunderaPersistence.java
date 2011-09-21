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

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

import org.apache.commons.lang.NotImplementedException;

import com.impetus.kundera.startup.KunderaLoadManager;

/**
 * The Class KunderaPersistence.
 * 
 * @author animesh.kumar
 */
@SuppressWarnings("unchecked")
public class KunderaPersistence implements PersistenceProvider
{

    /**
     * Instantiates a new kundera persistence.
     */
    public KunderaPersistence()
    {
    }

    /*
     * @see
     * javax.persistence.spi.PersistenceProvider#createContainerEntityManagerFactory
     * (javax.persistence.spi.PersistenceUnitInfo, java.util.Map)
     */
    @Override
    public final EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map)
    {
        return createEntityManagerFactory(info.getPersistenceUnitName(), map);
    }

    /*
     * @see
     * javax.persistence.spi.PersistenceProvider#createEntityManagerFactory(
     * java.lang.String, java.util.Map)
     */
    @Override
    public final EntityManagerFactory createEntityManagerFactory(String persistenceUnit, Map map)
    {
        // Invoke all Kundera Related Initiallization Tasks
        KunderaLoadManager loadManager = new KunderaLoadManager();
        loadManager.loadMetadata(persistenceUnit);      
        
        EntityManagerFactoryBuilder builder = new EntityManagerFactoryBuilder();
        return builder.buildEntityManagerFactory(persistenceUnit, map);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceProvider#getProviderUtil()
     */
    @Override
    public ProviderUtil getProviderUtil()
    {
        throw new NotImplementedException("TODO");
    } 
    

}
