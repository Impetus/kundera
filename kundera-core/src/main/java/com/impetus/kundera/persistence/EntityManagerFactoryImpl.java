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
package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContextType;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaPersistenceUnitUtil;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.PersistenceUtilHelper;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.cache.NonOperationalCacheProvider;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.loader.ClientLifeCycleManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;

/**
 * Implementation class for {@link EntityManagerFactory}
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
    /** The cache provider. */
    private CacheProvider cacheProvider;

    /**
     * Array of persistence units. (Contains only one string usually except when
     * persisting in multiple data-stores)
     */
    private String[] persistenceUnits;

    // Transaction type
    PersistenceUnitTransactionType transactionType;

    private final KunderaPersistenceUnitUtil util;

    private final PersistenceUtilHelper.MetadataCache cache = new PersistenceUtilHelper.MetadataCache();

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
     * @param properties
     *            the properties
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
        cacheProvider.createCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);

        // Invoke Client Loaders
        // logger.info("Loading Client(s) For Persistence Unit(s) " +
        // persistenceUnit);

        Set<PersistenceUnitTransactionType> txTypes = new HashSet<PersistenceUnitTransactionType>();

        for (String pu : persistenceUnits)
        {
            PersistenceUnitTransactionType txType = KunderaMetadataManager.getPersistenceUnitMetadata(pu)
                    .getTransactionType();
            txTypes.add(txType);
        }

        if (txTypes.size() != 1)
        {
            throw new IllegalArgumentException(
                    "For polyglot persistence, it is mandatory for all persistence units to have same Transction type.");
        }
        else
        {
            this.transactionType = txTypes.iterator().next();
        }

        this.util = new KunderaPersistenceUnitUtil(cache);

        logger.info("EntityManagerFactory created for persistence unit : " + persistenceUnit);
    }

    /**
     * Close the factory, releasing any resources that it holds. After a factory
     * instance has been closed, all methods invoked on it will throw the
     * IllegalStateException, except for isOpen, which will return false. Once
     * an EntityManagerFactory has been closed, all its entity managers are
     * considered to be in the closed state.
     * 
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#close()
     */
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
            ((ClientLifeCycleManager) ClientResolver.getClientFactory(pu)).destroy();
        }
    }

    /**
     * Create a new application-managed EntityManager. This method returns a new
     * EntityManager instance each time it is invoked. The isOpen method will
     * return true on the returned instance.
     * 
     * @return entity manager instance
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     */
    @Override
    public final EntityManager createEntityManager()
    {
        // For Application managed persistence context, type is always EXTENDED
        if (isOpen())
        {
            return new EntityManagerImpl(this, transactionType, PersistenceContextType.EXTENDED);
        }
        throw new IllegalStateException("entity manager factory has been closed");
    }

    /**
     * Create a new application-managed EntityManager with the specified Map of
     * properties. This method returns a new EntityManager instance each time it
     * is invoked. The isOpen method will return true on the returned instance.
     * 
     * @param map
     *            properties for entity manager
     * @return entity manager instance
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     */
    @Override
    public final EntityManager createEntityManager(Map map)
    {
        // For Application managed persistence context, type is always EXTENDED
        if (isOpen())
        {
            return new EntityManagerImpl(this, map, transactionType, PersistenceContextType.EXTENDED);
        }
        throw new IllegalStateException("entity manager factory has been closed");
    }

    /**
     * Indicates whether the factory is open. Returns true until the factory has
     * been closed.
     * 
     * @return boolean indicating whether the factory is open
     * @see javax.persistence.EntityManagerFactory#isOpen()
     */
    @Override
    public final boolean isOpen()
    {
        return !closed;
    }

    /**
     * Return an instance of CriteriaBuilder for the creation of CriteriaQuery
     * objects.
     * 
     * @return CriteriaBuilder instance
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#getCriteriaBuilder()
     */
    @Override
    public CriteriaBuilder getCriteriaBuilder()
    {
        throw new NotImplementedException("Criteria Query currently not supported by Kundera");
    }

    /**
     * Return an instance of Metamodel interface for access to the metamodel of
     * the persistence unit.
     * 
     * @return Metamodel instance
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#getMetamodel()
     */
    @Override
    public Metamodel getMetamodel()
    {
        return KunderaMetadataManager.getMetamodel(getPersistenceUnits());
    }

    /**
     * Get the properties and associated values that are in effect for the
     * entity manager factory. Changing the contents of the map does not change
     * the configuration in effect.
     * 
     * @return properties
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#getProperties()
     */
    @Override
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    /**
     * Access the cache that is associated with the entity manager factory (the
     * "second level cache").
     * 
     * @return instance of the Cache interface
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#getCache()
     */
    @Override
    public Cache getCache()
    {
        return cacheProvider.getCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);
    }

    /**
     * Return interface providing access to utility methods for the persistence
     * unit.
     * 
     * @return PersistenceUnitUtil interface
     * @throws IllegalStateException
     *             if the entity manager factory has been closed
     * @see javax.persistence.EntityManagerFactory#getPersistenceUnitUtil()
     */
    @Override
    public PersistenceUnitUtil getPersistenceUnitUtil()
    {
        if (!isOpen())
        {
            throw new IllegalStateException("EntityManagerFactory is closed");
        }
        return this.util;
    }

    /**
     * @return the transactionType
     */
    public PersistenceUnitTransactionType getTransactionType()
    {
        return transactionType;
    }

    /**
     * @param transactionType
     *            the transactionType to set
     */
    public void setTransactionType(PersistenceUnitTransactionType transactionType)
    {
        this.transactionType = transactionType;
    }

    /**
     * Inits the second level cache.
     * 
     * @return the cache provider
     */
    @SuppressWarnings("unchecked")
    private CacheProvider initSecondLevelCache()
    {
        String classResourceName = (String) getProperties().get(PersistenceProperties.KUNDERA_CACHE_CONFIG_RESOURCE);
        String cacheProviderClassName = (String) getProperties()
                .get(PersistenceProperties.KUNDERA_CACHE_PROVIDER_CLASS);

        CacheProvider cacheProvider = null;
        if (cacheProviderClassName != null)
        {

            try
            {
                Class<CacheProvider> cacheProviderClass = (Class<CacheProvider>) Class.forName(cacheProviderClassName);
                cacheProvider = cacheProviderClass.newInstance();
                cacheProvider.init(classResourceName);
            }
            catch (ClassNotFoundException e)
            {
                throw new CacheException("Could not find class " + cacheProviderClassName
                        + ". Check whether you spelled it correctly in persistence.xml", e);
            }
            catch (InstantiationException e)
            {
                throw new CacheException("Could not instantiate " + cacheProviderClassName, e);
            }
            catch (IllegalAccessException e)
            {
                throw new CacheException(e);
            }

        }
        if (cacheProvider == null)
        {
            cacheProvider = new NonOperationalCacheProvider();
        }
        return cacheProvider;
    }

    /**
     * Gets the persistence units.
     * 
     * @return the persistence units
     */
    String[] getPersistenceUnits()
    {
        return persistenceUnits;
    }

}
