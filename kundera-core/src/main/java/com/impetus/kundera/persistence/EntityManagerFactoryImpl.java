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

    private CacheProvider cacheProvider;

    private String persistenceUnit;

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
        // TODO Device some better (JPA) way
        properties.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        this.properties = properties;
        this.persistenceUnit = persistenceUnit;
        logger.info("EntityManagerFactory created for persistence unit : " + persistenceUnit);
    }

    @Override
    public final void close()
    {
        closed = true;
        cacheProvider.shutdown();
        ClientResolver.getClientFactory(getPersistenceUnit()).unload(getPersistenceUnit());
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

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManagerFactory#getMetamodel()
     */
    @Override
    public Metamodel getMetamodel()
    {
        return KunderaMetadataManager.getMetamodel(getPersistenceUnit());
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
            String resourceName = (String) getProperties().get("kundera.cache.config.resource");
            cacheProvider = initSecondLevelCache((String) getProperties().get("kundera.cache.provider.class"),
                    resourceName);

            return cacheProvider.createCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);
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
    private CacheProvider initSecondLevelCache(String cacheProviderClassName, String classResourceName)
    {
        // String cacheProviderClassName = (String)
        // props.get("kundera.cache.provider_class");
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

    private String getPersistenceUnit()
    {
        return persistenceUnit;
    }

}
