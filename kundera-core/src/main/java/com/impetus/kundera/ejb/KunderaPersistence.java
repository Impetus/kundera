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

import com.impetus.kundera.loader.ClientResolver;
import com.impetus.kundera.loader.ClientType;
import com.impetus.kundera.startup.ApplicationLoader;
import com.impetus.kundera.startup.CoreLoader;
import com.impetus.kundera.startup.model.KunderaMetadata;
import com.impetus.kundera.startup.model.PersistenceUnitMetadata;

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
        // Load Core
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
        // Initialize Persistence Unit Related Parts
        initializeKundera(persistenceUnit);

        EntityManagerFactoryBuilder builder = new EntityManagerFactoryBuilder();
        return builder.buildEntityManagerFactory(persistenceUnit, map);
    }

    private void initializeKundera(String persistenceUnit)
    {
        // Invoke Application MetaData
        (new ApplicationLoader()).load(persistenceUnit);

        // Invoke Client Loaders
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.getInstance().getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String kunderaClientName = (String) persistenceUnitMetadata.getProperties().get("kundera.client");
        ClientType clientType = ClientType.getValue(kunderaClientName.toUpperCase());
        ClientResolver.getClientLoader(clientType).load(persistenceUnit);
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
