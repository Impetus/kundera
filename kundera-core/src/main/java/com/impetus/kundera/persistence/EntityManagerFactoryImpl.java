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
package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceUnitInfo;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.cache.NonOperationalCacheProvider;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.metadata.KunderaMetadataManager;

/**
 * The Class EntityManagerFactoryImpl.
 * 
 * @author animesh.kumar
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory
{

    /** the log used by this class. */
    private static Log logger = LogFactory.getLog(EntityManagerFactoryImpl.class);

    /** Whether or not the factory has been closed. */
    private boolean closed = false;

    /**
     * Persistence Unit Properties Overriden by user provided factory
     * properties.
     */
    private Map<String, Object> properties;

    // TODO: Move it to Application Metadata
    private CacheProvider cacheProvider;

    /**
     * Array of persistence units. (Contains only one string usually except when
     * persisting in multiple data-stores)
     */
    String[] persistenceUnits;

    /**
     * This one is generally called via the PersistenceProvider.
     * 
     * @param persistenceUnitInfo
     *            only using persistenceUnit for now
     * @param props
     *            the props
     */
    public EntityManagerFactoryImpl(PersistenceUnitInfo persistenceUnitInfo, Map props)
    {
        this(persistenceUnitInfo != null ? persistenceUnitInfo.getPersistenceUnitName() : null, props);
    }

    /**
     * Use this if you want to construct this directly.
     * 
     * @param persistenceUnit
     *            used to prefix the Cassandra domains
     * @param props
     *            should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnit, Map<String, Object> properties)
    {
        if (properties == null)
        {
            properties = new HashMap<String, Object>();
        }

        // TODO Devise some better (JPA) way
        properties.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        this.properties = properties;
        this.persistenceUnits = persistenceUnit.split(Constants.PERSISTENCE_UNIT_SEPARATOR);

        // Initialize L2 cache
        cacheProvider = initSecondLevelCache();
        try
        {
            cacheProvider.createCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);
        }
        catch (CacheException e)
        {
            logger.warn("Error while creating L2 cache. Entities won't be stored in L2 cache. Details:"
                    + e.getMessage());
        }

        logger.info("EntityManagerFactory created for persistence unit : " + persistenceUnit);
    }

    @Override
    public final void close()
    {
        closed = true;

        // Shut cache provider down
        if (cacheProvider != null)
        {
            cacheProvider.shutdown();
        }

        for (String pu : persistenceUnits)
        {
            ClientResolver.getClientFactory(pu).unload(pu);
        }
    }

    @Override
    public final EntityManager createEntityManager()
    {
        return new EntityManagerImpl(this);
    }

    @Override
    public final EntityManager createEntityManager(Map map)
    {
        return new EntityManagerImpl(this, map);
    }

    @Override
    public final boolean isOpen()
    {
        return !closed;
    }

    @Override
    public CriteriaBuilder getCriteriaBuilder()
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public Metamodel getMetamodel()
    {
        return KunderaMetadataManager.getMetamodel(getPersistenceUnits());
    }

    @Override
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    @Override
    public Cache getCache()
    {
        try
        {
            return cacheProvider.getCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);
        }
        catch (CacheException e)
        {
            logger.error("Error while getting cache. Details:" + e.getMessage());
            return null;
        }
    }

    @Override
    public PersistenceUnitUtil getPersistenceUnitUtil()
    {
        throw new NotImplementedException("TODO");
    }

    /**
     * Inits the second level cache.
     * 
     * @param cacheProviderClassName
     *            the cache provider class name
     * @param classResourceName
     *            the class resource name
     * @return the cache provider
     */
    @SuppressWarnings("unchecked")
    private CacheProvider initSecondLevelCache()
    {
        String classResourceName = (String) getProperties().get("kundera.cache.config.resource");
        String cacheProviderClassName = (String) getProperties().get("kundera.cache.provider.class");

        CacheProvider cacheProvider = null;
        if (cacheProviderClassName != null)
        {
            try
            {
                Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
                cacheProvider = cacheProviderClass.newInstance();
                cacheProvider.init(classResourceName);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        if (cacheProvider == null)
        {
            cacheProvider = new NonOperationalCacheProvider();
        }
        return cacheProvider;
    }

    private String[] getPersistenceUnits()
    {
        return persistenceUnits;
    }

}
