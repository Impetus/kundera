/*
 * Copyright (c) 2010-2011, Animesh Kumar
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.CassandraEntityManager;
import com.impetus.kundera.db.DataManager;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.LuceneQuery;

/**
 * @author animesh.kumar
 */
public class EntityManagerImpl implements CassandraEntityManager {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerImpl.class);

	private EntityManagerFactoryImpl factory;

	/**
	 * cache is used to store objects retrieved in this EntityManager session
	 */
	private Map<Object, Object> sessionCache;
	private boolean sessionless;
	private boolean closed = false;

	private CassandraClient client;
	private DataManager dataManager;
	private IndexManager indexManager;
	private MetadataManager metadataManager;
	
    private String persistenceUnitName;

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

	@Override
	public void remove(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());
			
			String id = PropertyAccessorFactory.getStringProperty(entity, metadata.getIdProperty());
			
			removeFromCache(entity.getClass(), id);
			dataManager.remove(metadata, entity, id);
			indexManager.remove(metadata, entity, id);
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
	}

	@Override
	public <T> T find(Class<T> entityClass, Object primaryKey) {
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

	@Override
	public <T> T merge(T entity) {
		if (entity == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());
			// TODO Improve this bit. should we not implement some merge on
			// DataManager?
			dataManager.persist(metadata, entity);
			indexManager.update(metadata, entity);
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
		return entity;
	}

	@Override
	public void persist(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entity.getClass());
			dataManager.persist(metadata, entity);
			indexManager.write(metadata, entity);
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
	}

	@Override
	public <T> List<T> find(Class<T> entityClass, Object... primaryKeys) {
		try {
			String[] ids = Arrays.asList(primaryKeys).toArray(new String[] {});
			EntityMetadata metadata = factory.getMetadataManager().getEntityMetadata(entityClass);
			return dataManager.find(metadata, entityClass, ids);
		} catch (Exception e) {
			throw new PersistenceException(e.getMessage());
		}
	}

	@Override
	public void clear() {
		checkClosed();
		// this is really only useful with transactions
		if (sessionCache != null) {
			sessionCache = new ConcurrentHashMap<Object, Object>();
		}
	}

	@Override
	public void close() {
		closed = true;
		sessionCache = null;
	}

	@Override
	public boolean contains(Object entity) {
		return false;
	}

	@Override
	public Query createNamedQuery(String name) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query createNativeQuery(String sqlString) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query createNativeQuery(String sqlString, Class resultClass) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query createNativeQuery(String sqlString, String resultSetMapping) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public Query createQuery(String ejbqlString) {
		return new LuceneQuery(this, ejbqlString);
	}

	@Override
	public void flush() {
		// always flushed to cassandra anyway! relax.
	}

	@Override
	public Object getDelegate() {
		return null;
	}

	@Override
	public FlushModeType getFlushMode() {
		throw new NotImplementedException("TODO");
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public EntityTransaction getTransaction() {
		return new EntityTransactionImpl();
	}

	@Override
	public boolean isOpen() {
		return sessionless || !closed;
	}

	@Override
	public void joinTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void lock(Object entity, LockModeType lockMode) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public void refresh(Object entity) {
		throw new NotImplementedException("TODO");
	}

	@Override
	public void setFlushMode(FlushModeType flushMode) {
		throw new NotImplementedException("TODO");
	}

	private void checkClosed() {
		if (!isOpen()) {
			throw new IllegalStateException("EntityManager has been closed.");
		}
	}

	/**
	 * @return the metadataManager
	 */
	@Override
	public MetadataManager getMetadataManager() {
		return metadataManager;
	}

	/**
	 * @return the dataManager
	 */
	public DataManager getDataManager() {
		return dataManager;
	}

	/**
	 * @return the indexManager
	 */
	public IndexManager getIndexManager() {
		return indexManager;
	}

	/**
	 * @return the client
	 */
	@Override
	public CassandraClient getClient() {
		return client;
	}
	
	public String getPersistenceUnitName() {
		return persistenceUnitName;
	}

	// helper methods for session-level-cache
    private <T> T findInCache (Class<T> entityClass, Object id) {
    	if (null == sessionCache) return null;
    	
        String key = cacheKey(entityClass, id);
        log.debug("Looking @Entity from cache with cachekey: " + key);
        T o = (T) sessionCache.get(key);
        return o;
    }

    private void saveToCache (Object id, Object entity) {
    	if (null == sessionCache) return;
    	
        String key = cacheKey(entity.getClass(), id);
        log.debug("Putting @Entity in cache with cachekey:" + key);
        sessionCache.put(key, entity);
    }

	private <T> void removeFromCache(Class<T> entityClass, Object id) {
		if (null == sessionCache) return;
		
		String key = cacheKey(entityClass, id);
		log.debug("Removing @Entity from cache with cachekey:" + key);
		sessionCache.remove(key);
	}

    private String cacheKey(Class<?> clazz, Object id) {
        return clazz.getName() + "_" + id;
    }
	
}
