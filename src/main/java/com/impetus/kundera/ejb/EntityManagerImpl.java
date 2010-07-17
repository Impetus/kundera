/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ejb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceException;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.CassandraEntityManager;
import com.impetus.kundera.db.DataManager;
import com.impetus.kundera.ejb.event.CallbackMethod;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.LuceneQuery;

/**
 * The Class EntityManagerImpl.
 * 
 * @author animesh.kumar
 */
public class EntityManagerImpl implements CassandraEntityManager {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerImpl.class);

    /** The factory. */
    private EntityManagerFactoryImpl factory;

    /** cache is used to store objects retrieved in this EntityManager session. */
    private Map<Object, Object> sessionCache;

    /** The sessionless. */
    private boolean sessionless;

    /** The closed. */
    private boolean closed = false;

    /** The client. */
    private CassandraClient client;

    /** The data manager. */
    private DataManager dataManager;

    /** The index manager. */
    private IndexManager indexManager;

    /** The metadata manager. */
    private MetadataManager metadataManager;

    /** The persistence unit name. */
    private String persistenceUnitName;

    /**
     * Instantiates a new entity manager impl.
     * 
     * @param factory
     *            the factory
     * @param client
     *            the client
     * @param sessionless
     *            the sessionless
     */
    public EntityManagerImpl(EntityManagerFactoryImpl factory, CassandraClient client, boolean sessionless) {
        this.factory = factory;
        this.metadataManager = factory.getMetadataManager();
        this.client = client;
        this.sessionless = sessionless;

        if (!sessionless) {
            sessionCache = new ConcurrentHashMap<Object, Object>();
        }
        this.persistenceUnitName = factory.getPersistenceUnitName();

        dataManager = new DataManager(this);
        indexManager = new IndexManager(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#remove(java.lang.Object)
     */
	@Override
	public void remove(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());

			String id = PropertyAccessorHelper.getId(entity, metadata);

			// fire PreRemove events
			fireJPAEventListeners(metadata, entity, PreRemove.class);

			removeFromCache(entity.getClass(), id);
			dataManager.remove(metadata, entity, id);
			indexManager.remove(metadata, entity, id);

			// fire PostRemove events
			fireJPAEventListeners(metadata, entity, PostRemove.class);
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
	}

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public final <T> T find(Class<T> entityClass, Object primaryKey) {
        if (!sessionless && closed) {
            throw new PersistenceException("EntityManager already closed.");
        }
        if (primaryKey == null) {
            throw new IllegalArgumentException("primaryKey value must not be null.");
        }

        T t = null;
        t = findInCache(entityClass, primaryKey);
        if (null != t) {
            log.debug("@Entity " + entityClass.getName() + " for id:" + primaryKey + " is found in cache.");
            return t;
        }

        EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entityClass);
        String id = primaryKey.toString();

        try {
            T entity = dataManager.find(metadata, entityClass, id);
            if (null != entity) {
                saveToCache(primaryKey, entity);
            }
            return entity;
        } catch (Exception e) {
            throw new PersistenceException(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#merge(java.lang.Object)
     */
    @Override
    public final <T> T merge(T entity) {
		if (entity == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());

			// fire PreUpdate events
			fireJPAEventListeners(metadata, entity, PreUpdate.class);

			dataManager.merge(metadata, entity);
			indexManager.update(metadata, entity);

			// fire PostUpdate events
			fireJPAEventListeners(metadata, entity, PostUpdate.class);

		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
		return entity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#persist(java.lang.Object)
     */
    @Override
    public final void persist(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        try {
        	
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());

			// fire pre-persist events
			fireJPAEventListeners(metadata, entity, PrePersist.class);

			dataManager.persist(metadata, entity);
			indexManager.write(metadata, entity);

			// fire post-persist events			
			fireJPAEventListeners(metadata, entity, PostPersist.class);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.CassandraEntityManager#find(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public final <T> List<T> find(Class<T> entityClass, Object... primaryKeys) {
        try {
            String[] ids = Arrays.asList(primaryKeys).toArray(new String[] {});
            EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entityClass);
            return dataManager.find(metadata, entityClass, ids);
        } catch (Exception e) {
            throw new PersistenceException(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#clear()
     */
    @Override
    public final void clear() {
        checkClosed();
        // this is really only useful with transactions
        if (sessionCache != null) {
            sessionCache = new ConcurrentHashMap<Object, Object>();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#close()
     */
    @Override
    public final void close() {
        closed = true;
        sessionCache = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#contains(java.lang.Object)
     */
    @Override
    public final boolean contains(Object entity) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNamedQuery(java.lang.String)
     */
    @Override
    public final Query createNamedQuery(String name) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String)
     */
    @Override
    public final Query createNativeQuery(String sqlString) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public final Query createNativeQuery(String sqlString, Class resultClass) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
     * java.lang.String)
     */
    @Override
    public final Query createNativeQuery(String sqlString, String resultSetMapping) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createQuery(java.lang.String)
     */
    @Override
    public final Query createQuery(String ejbqlString) {
        return new LuceneQuery(this, ejbqlString);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#flush()
     */
    @Override
    public final void flush() {
        // always flushed to cassandra anyway! relax.
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getDelegate()
     */
    @Override
    public final Object getDelegate() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getFlushMode()
     */
    @Override
    public final FlushModeType getFlushMode() {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getReference(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public final <T> T getReference(Class<T> entityClass, Object primaryKey) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getTransaction()
     */
    @Override
    public final EntityTransaction getTransaction() {
        return new EntityTransactionImpl();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#isOpen()
     */
    @Override
    public final boolean isOpen() {
        return sessionless || !closed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#joinTransaction()
     */
    @Override
    public final void joinTransaction() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#lock(java.lang.Object,
     * javax.persistence.LockModeType)
     */
    @Override
    public final void lock(Object entity, LockModeType lockMode) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#refresh(java.lang.Object)
     */
    @Override
    public final void refresh(Object entity) {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.EntityManager#setFlushMode(javax.persistence.FlushModeType
     * )
     */
    @Override
    public final void setFlushMode(FlushModeType flushMode) {
        throw new NotImplementedException("TODO");
    }

    /**
     * Check closed.
     */
    private void checkClosed() {
        if (!isOpen()) {
            throw new IllegalStateException("EntityManager has been closed.");
        }
    }

    /**
     * Gets the metadata manager.
     * 
     * @return the metadataManager
     */
    @Override
    public final MetadataManager getMetadataManager() {
        return metadataManager;
    }

    /**
     * Gets the data manager.
     * 
     * @return the dataManager
     */
    public final DataManager getDataManager() {
        return dataManager;
    }

    /**
     * Gets the index manager.
     * 
     * @return the indexManager
     */
    public final IndexManager getIndexManager() {
        return indexManager;
    }

    /**
     * Gets the client.
     * 
     * @return the client
     */
    @Override
    public final CassandraClient getClient() {
        return client;
    }

    /**
     * Gets the persistence unit name.
     * 
     * @return the persistence unit name
     */
    public final String getPersistenceUnitName() {
        return persistenceUnitName;
    }

    // helper methods for session-level-cache
    /**
     * Find in cache.
     * 
     * @param entityClass
     *            the entity class
     * @param id
     *            the id
     * 
     * @return the t
     */
    private <T> T findInCache(Class<T> entityClass, Object id) {
        if (null == sessionCache)
            return null;

        String key = cacheKey(entityClass, id);
        log.debug("Looking @Entity from cache with cachekey: " + key);
        T o = (T) sessionCache.get(key);
        return o;
    }

    /**
     * Save to cache.
     * 
     * @param id
     *            the id
     * @param entity
     *            the entity
     */
    private void saveToCache(Object id, Object entity) {
        if (null == sessionCache)
            return;

        String key = cacheKey(entity.getClass(), id);
        log.debug("Putting @Entity in cache with cachekey:" + key);
        sessionCache.put(key, entity);
    }

    /**
     * Removes the from cache.
     * 
     * @param entityClass
     *            the entity class
     * @param id
     *            the id
     */
    private <T> void removeFromCache(Class<T> entityClass, Object id) {
        if (null == sessionCache)
            return;

        String key = cacheKey(entityClass, id);
        log.debug("Removing @Entity from cache with cachekey:" + key);
        sessionCache.remove(key);
    }

    /**
     * Cache key.
     * 
     * @param clazz
     *            the clazz
     * @param id
     *            the id
     * 
     * @return the string
     */
    private String cacheKey(Class<?> clazz, Object id) {
        return clazz.getName() + "_" + id;
    }

	private void fireJPAEventListeners(EntityMetadata metadata, Object entity, Class<?> event) throws Exception {
		log.debug("Firing Callback methods on @Entity(" + entity.getClass().getName() + ") for Event(" + event.getSimpleName() + ")");
		// handler external listeners first
		List<? extends CallbackMethod> callBackMethods = metadata.getCallbackMethods(event);
		if (null != callBackMethods) {
			for (CallbackMethod callback : callBackMethods) {
				log.debug ("Firing (" + callback + ")");
				try {
					callback.invoke(entity);
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			}
		} 
	}
}
