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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceUnitInfo;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.proxy.EntityEnhancerFactory;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializerFactory;
import com.impetus.kundera.proxy.cglib.CglibEntityEnhancerFactory;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;

/**
 * The Class EntityManagerFactoryImpl.
 *
 * @author animesh.kumar
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory
{

    /** the log used by this class. */
    private static Log LOG = LogFactory.getLog(EntityManagerFactoryImpl.class);

    /** Whether or not the factory has been closed. */
    private boolean closed = false;

    /** Also the prefix that will be applied to each Domain. */
    private String persistenceUnitName;

    /** properties file values. */
    @SuppressWarnings("unchecked")
    private Map props;

    /** The sessionless. */
    private boolean sessionless;

    /** The metadata manager. */
    private MetadataManager metadataManager;

    /** The nodes. */
    private String[] nodes;

    /** The port. */
    private Integer port;

    /** The keyspace. */
    private String schema;

    /** The classes. */
    private List<String> classes;

    /** The Cache provider. */
    private CacheProvider cacheProvider;

    /** The cache provider class name. */
    private String cacheProviderClassName;

    /** The enhanced proxy factory. */
    private EntityEnhancerFactory enhancedProxyFactory;

    /** The lazy initializer factory. */
    private LazyInitializerFactory lazyInitializerFactory;

    /**
     * A convenience constructor.
     *
     * @param persistenceUnitName
     *            used to prefix the Cassandra domains
     */
    public EntityManagerFactoryImpl(String persistenceUnitName)
    {
        this(persistenceUnitName, null);
    }

    /**
     * This one is generally called via the PersistenceProvider.
     *
     * @param persistenceUnitInfo
     *            only using persistenceUnitName for now
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
     * @param persistenceUnitName
     *            used to prefix the Cassandra domains
     * @param props
     *            should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props)
    {
        onLoad(persistenceUnitName);
        this.props = props;
    }

    /**
     * Instantiates a new entity manager factory impl.
     *
     * @param metaData
     *            the meta data
     * @param props
     *            the props
     */
    public EntityManagerFactoryImpl(PersistenceMetadata metaData, Map props)
    {
        this.classes = metaData.getClasses();
        onLoad(metaData.getName());
        this.props = props;
    }

    /**
     * Method to instantiate persistence entities and metadata.
     *
     * @param persistenceUnitName
     *            the persistence unit name
     */
    private void onLoad(String persistenceUnitName)
    {
        if (persistenceUnitName == null)
        {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }
        long start = System.currentTimeMillis();

        this.persistenceUnitName = persistenceUnitName;
        metadataManager = new MetadataManager();

        // scan classes for @Entity
        Reader reader = new ClasspathReader(this.classes);
        reader.addValidAnnotations(Entity.class.getName());
        reader.addAnnotationDiscoveryListeners(metadataManager);
        reader.read();

        //metadataManager.build();

        enhancedProxyFactory = new CglibEntityEnhancerFactory();
        lazyInitializerFactory = new CglibLazyInitializerFactory();

        LOG.info("EntityManagerFactoryImpl loaded in " + (System.currentTimeMillis() - start) + "ms.");
    }

    /**
     * Gets the cache.
     *
     * @param entity
     *            the entity
     * @return the cache
     */
    public Cache getCache(Class<?> entity)
    {
        try
        {
            String cacheName = metadataManager.getEntityMetadata(entity).getEntityClazz().getName();
            return cacheProvider.createCache(cacheName);
        }
        catch (CacheException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the metadata manager.
     *
     * @return the metadataManager
     */
    public final MetadataManager getMetadataManager()
    {
        return metadataManager;
    }

    /* @see javax.persistence.EntityManagerFactory#close() */
    /*
     * (non-Javadoc)
     *
     * @see javax.persistence.EntityManagerFactory#close()
     */
    @Override
    public final void close()
    {
        closed = true;
        // client.shutdown();
        cacheProvider.shutdown();
    }

    /* @see javax.persistence.EntityManagerFactory#createEntityManager() */
    /*
     * (non-Javadoc)
     *
     * @see javax.persistence.EntityManagerFactory#createEntityManager()
     */
    @Override
    public final EntityManager createEntityManager()
    {
        return new EntityManagerImpl(this);
    }

    /*
     * @see
     * javax.persistence.EntityManagerFactory#createEntityManager(java.util.Map)
     */
    /*
     * (non-Javadoc)
     *
     * @see
     * javax.persistence.EntityManagerFactory#createEntityManager(java.util.Map)
     */
    @Override
    public final EntityManager createEntityManager(Map map)
    {
        return new EntityManagerImpl(this);
    }

    /**
     * Gets the enhanced entity.
     *
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param foreignKeyMap
     *            the foreign key map
     * @return the enhanced entity
     */
    public EnhancedEntity getEnhancedEntity(Object entity, String id, Map<String, Set<String>> foreignKeyMap)
    {
        return enhancedProxyFactory.getProxy(entity, id, foreignKeyMap);
    }

    /**
     * Gets the lazy entity.
     *
     * @param entityName
     *            the entity name
     * @param persistentClass
     *            the persistent class
     * @param getIdentifierMethod
     *            the get identifier method
     * @param setIdentifierMethod
     *            the set identifier method
     * @param id
     *            the id
     * @param em
     *            the em
     * @return the lazy entity
     */
    public KunderaProxy getLazyEntity(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, String id, EntityManagerImpl em)
    {
        return lazyInitializerFactory.getProxy(entityName, persistentClass, getIdentifierMethod, setIdentifierMethod,
                id, em);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.persistence.EntityManagerFactory#isOpen()
     */
    @Override
    public final boolean isOpen()
    {
        return !closed;
    }

    /**
     * Gets the persistence unit name.
     *
     * @return the persistence unit name
     */
    public final String getPersistenceUnitName()
    {
        return persistenceUnitName;
    }

    /**
     * Gets the nodes.
     *
     * @return the nodes
     */
    public String[] getNodes()
    {
        return nodes;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Gets the schema.
     *
     * @return the schema
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     * Sets the schema.
     *
     * @param schema
     *            the schema to set
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Gets the classes.
     *
     * @return the classes
     */
    public List<String> getClasses()
    {
        return classes;
    }

    /**
     * Sets the classes.
     *
     * @param classes
     *            the classes to set
     */
    public void setClasses(List<String> classes)
    {
        this.classes = classes;
    }

    /* (non-Javadoc)
     * @see javax.persistence.EntityManagerFactory#getCriteriaBuilder()
     */
    @Override
    public CriteriaBuilder getCriteriaBuilder()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.EntityManagerFactory#getMetamodel()
     */
    @Override
    public Metamodel getMetamodel()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.EntityManagerFactory#getProperties()
     */
    @Override
    public Map<String, Object> getProperties()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.EntityManagerFactory#getCache()
     */
    @Override
    public javax.persistence.Cache getCache()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.EntityManagerFactory#getPersistenceUnitUtil()
     */
    @Override
    public PersistenceUnitUtil getPersistenceUnitUtil()
    {
        throw new NotImplementedException("TODO");
    }  
    

}
