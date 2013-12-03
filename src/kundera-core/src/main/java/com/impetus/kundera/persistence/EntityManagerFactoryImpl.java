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
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.Cache;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContextType;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.SynchronizationType;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaPersistenceUnitUtil;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.PersistenceUtilHelper;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.cache.NonOperationalCacheProvider;
import com.impetus.kundera.client.ClientResolverException;
import com.impetus.kundera.configure.ClientMetadataBuilder;
import com.impetus.kundera.loader.ClientFactory;
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
    private static Logger logger = LoggerFactory.getLogger(EntityManagerFactoryImpl.class);

    /** Whether or not the factory has been closed. */
    private boolean closed;

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
    private PersistenceUnitTransactionType transactionType;

    private final KunderaPersistenceUnitUtil util;

    private final PersistenceUtilHelper.MetadataCache cache = new PersistenceUtilHelper.MetadataCache();

    /** ClientFactory map holds one clientfactory for one persistence unit */
    private Map<String, ClientFactory> clientFactories = new ConcurrentHashMap<String, ClientFactory>();

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
        Map<String, Object> propsMap = new HashMap<String, Object>();
        
        if (properties != null)
        {
            propsMap.putAll(properties);
        }

        // TODO Devise some better (JPA) way
        propsMap.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        this.properties = propsMap;
        this.persistenceUnits = persistenceUnit.split(Constants.PERSISTENCE_UNIT_SEPARATOR);

        // configure client factories
        configureClientFactories();

        // Initialize L2 cache
        this.cacheProvider = initSecondLevelCache();
        this.cacheProvider.createCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);

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

        if (logger.isDebugEnabled())
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
        if (isOpen())
        {
            closed = true;

            // Shut cache provider down
            if (cacheProvider != null)
            {
                cacheProvider.shutdown();
            }

            for (String pu : persistenceUnits)
            {
                ((ClientLifeCycleManager) clientFactories.get(pu)).destroy();
//                KunderaMetadata.INSTANCE.unloadKunderaMetadata(pu);
            }
            this.persistenceUnits = null;
            this.properties = null;
            clientFactories.clear();
            clientFactories = new ConcurrentHashMap<String, ClientFactory>();
        }
        else
        {
            throw new IllegalStateException("entity manager factory has been closed");
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
        if (isOpen())
        {
            throw new NotImplementedException("Criteria Query currently not supported by Kundera");
        }
        throw new IllegalStateException("entity manager factory has been closed");
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
        if (isOpen())
        {
            return KunderaMetadataManager.getMetamodel(getPersistenceUnits());
        }
        throw new IllegalStateException("entity manager factory has been closed");
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
        if (isOpen())
        {
            return properties;
        }
        throw new IllegalStateException("entity manager factory has been closed");
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
        if (isOpen())
        {
            return cacheProvider.getCache(Constants.KUNDERA_SECONDARY_CACHE_NAME);
        }
        throw new IllegalStateException("entity manager factory has been closed");
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
     * Initialize and load clientFactory for all persistenceUnit with external
     * properties.
     * 
     * @param persistenceUnit
     * @param externalProperties
     */
    private void configureClientFactories()
    {
        ClientMetadataBuilder builder = new ClientMetadataBuilder(getProperties(), getPersistenceUnits());
        builder.buildClientFactoryMetadata(clientFactories);
    }

    /**
     * Inits the second level cache.
     * 
     * @return the cache provider
     */
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

    ClientFactory getClientFactory(final String pu)
    {
        ClientFactory clientFactory = clientFactories.get(pu);
        if (clientFactory != null)
        {
            return clientFactory;
        }
        logger.error("Client Factory Not Configured For Specified Client Type : ");
        throw new ClientResolverException("Client Factory Not Configured For Specified Client Type.");
    }

    @Override
    public EntityManager createEntityManager(SynchronizationType paramSynchronizationType)
    {
        return createEntityManager();
    }

    @Override
    public EntityManager createEntityManager(SynchronizationType paramSynchronizationType, Map paramMap)
    {
        // TODO Auto-generated method stub
        return createEntityManager(paramMap);
    }

    @Override
    public void addNamedQuery(String paramString, Query paramQuery)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
    }

    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <T> void addNamedEntityGraph(String paramString, EntityGraph<T> paramEntityGraph)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        
    }
}
