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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.CascadeType;
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
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.proxy.EnhancedEntity;
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

	/** The entity resolver. */
	private EntityResolver entityResolver;

	/** The session. */
	private EntityManagerSession session;
	
	/** The event dispatcher. */
	private EntityEventDispatcher eventDispatcher;

	/**
	 * Instantiates a new entity manager impl.
	 * 
	 * @param factory
	 *            the factory
	 * @param client
	 *            the client
	 */
	public EntityManagerImpl(EntityManagerFactoryImpl factory,
			CassandraClient client) {
		this.factory = factory;
		this.metadataManager = factory.getMetadataManager();
		this.client = client;
		this.persistenceUnitName = factory.getPersistenceUnitName();
		dataManager = new DataManager(this);
		indexManager = new IndexManager(this);
		entityResolver = new EntityResolver(this);
		session = new EntityManagerSession(this);
		eventDispatcher = new EntityEventDispatcher();
	}

	/**
	 * Gets the factory.
	 * 
	 * @return the factory
	 */
	public EntityManagerFactoryImpl getFactory() {
		return factory;
	}

	/*
	 * @see javax.persistence.EntityManager#find(java.lang.Class,
	 * java.lang.Object)
	 */
	@Override
	public final <E> E find(Class<E> entityClass, Object primaryKey) {
		if (closed) {
			throw new PersistenceException("EntityManager already closed.");
		}
		if (primaryKey == null) {
			throw new IllegalArgumentException(
					"primaryKey value must not be null.");
		}
		
		// Validate
		metadataManager.validate(entityClass);
		
		E e = null;
		e = session.lookup(entityClass, primaryKey);
		if (null != e) {
			log.debug(entityClass.getName() + "_" + primaryKey
					+ " is loaded from cache!");
			return e;
		}

		return immediateLoadAndCache(entityClass, primaryKey);
	}

	/**
	 * Immediate load and cache.
	 * 
	 * @param <E>
	 *            the element type
	 * @param entityClass
	 *            the entity class
	 * @param primaryKey
	 *            the primary key
	 * @return the e
	 */
	protected <E> E immediateLoadAndCache(Class<E> entityClass, Object primaryKey) {
		try {
			EntityMetadata m = metadataManager.getEntityMetadata(entityClass);
			E e = dataManager.find(entityClass, m, primaryKey.toString());
			session.store(primaryKey, e, m.isCacheable());
			return e;
		} catch (Exception exp) {
			exp.printStackTrace();
			throw new PersistenceException(exp.getMessage());
		}
	}

	/*
	 * @see com.impetus.kundera.CassandraEntityManager#find(java.lang.Class,
	 * java.lang.Object[])
	 */
	@Override
	public final <E> List<E> find(Class<E> entityClass, Object... primaryKeys) {
		if (closed) {
			throw new PersistenceException("EntityManager already closed.");
		}
		if (primaryKeys == null) {
			throw new IllegalArgumentException(
					"primaryKey value must not be null.");
		}

		// Validate
		metadataManager.validate(entityClass);

		if (null == primaryKeys || primaryKeys.length == 0) {
			return new ArrayList<E>();
		}

		// TODO: load from cache first

		try {
			String[] ids = Arrays.asList(primaryKeys).toArray(new String[] {});
			EntityMetadata m = metadataManager.getEntityMetadata(entityClass);

			List<E> entities = dataManager.find(entityClass, m, ids);

			// TODO: cache entities for future lookup
			return entities;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PersistenceException(e.getMessage());
		}
	}

	/* @see javax.persistence.EntityManager#remove(java.lang.Object) */
	@Override
	public final void remove(Object e) {
		if (e == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}
		
		// Validate
		metadataManager.validate(e.getClass());

		try {

			List<EnhancedEntity> reachableEntities = entityResolver
					.resolve(e, CascadeType.REMOVE);

			// remove each one
			for (EnhancedEntity o : reachableEntities) {
				log.debug("Removing @Entity >> " + o);

				EntityMetadata m = metadataManager.getEntityMetadata(o
						.getEntity().getClass());

				// fire PreRemove events
				eventDispatcher.fireEventListeners (m, o, PreRemove.class);

				session.remove(o.getEntity().getClass(), o.getId());
				dataManager.remove(o, m);
				indexManager.remove(m, o.getEntity(), o.getId());

				// fire PostRemove events
				eventDispatcher.fireEventListeners (m, o, PostRemove.class);
			}
		} catch (Exception exp) {
			exp.printStackTrace();
			throw new PersistenceException(exp.getMessage());
		}
	}

	/* @see javax.persistence.EntityManager#merge(java.lang.Object) */
	@Override
	public final <E> E merge(E e) {
		if (e == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {

			List<EnhancedEntity> reachableEntities = entityResolver
					.resolve(e, CascadeType.MERGE);

			// save each one
			for (EnhancedEntity o : reachableEntities) {
				log.debug("Merging @Entity >> " + o);

				EntityMetadata metadata = metadataManager.getEntityMetadata(o
						.getEntity().getClass());

				// fire PreUpdate events
				eventDispatcher.fireEventListeners(metadata, o, PreUpdate.class);

				dataManager.merge(o, metadata);
				indexManager.update(metadata, o.getEntity());

				// fire PreUpdate events
				eventDispatcher.fireEventListeners(metadata, o, PostUpdate.class);
			}
		} catch (Exception exp) {
			exp.printStackTrace();
			throw new PersistenceException(exp.getMessage());
		}

		return e;
	}

	/* @see javax.persistence.EntityManager#persist(java.lang.Object) */
	@Override
	public final void persist(Object e) {
		if (e == null) {
			throw new IllegalArgumentException("Entity must not be null.");
		}

		try {
			// validate
			metadataManager.validate(e.getClass());

			List<EnhancedEntity> reachableEntities = entityResolver
					.resolve(e, CascadeType.PERSIST);

			// save each one
			for (EnhancedEntity o : reachableEntities) {
				log.debug("Persisting @Entity >> " + o);

				EntityMetadata metadata = metadataManager.getEntityMetadata(o
						.getEntity().getClass());

				// fire pre-persist events
				eventDispatcher.fireEventListeners(metadata, o, PrePersist.class);

				dataManager.persist(o, metadata);
				indexManager.write(metadata, o.getEntity());

				// fire post-persist events
				eventDispatcher.fireEventListeners(metadata, o, PostPersist.class);
			}
		} catch (Exception exp) {
			throw new PersistenceException(exp.getMessage());
		}
	}
	
	/* @see javax.persistence.EntityManager#clear() */
	@Override
	public final void clear() {
		checkClosed();
		session.clear();
	}

	/* @see javax.persistence.EntityManager#close() */
	@Override
	public final void close() {
		closed = true;
		session = null;
	}

	/* @see javax.persistence.EntityManager#contains(java.lang.Object) */
	@Override
	public final boolean contains(Object entity) {
		return false;
	}

	/* @see javax.persistence.EntityManager#createNamedQuery(java.lang.String) */
	@Override
	public final Query createNamedQuery(String name) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.EntityManager#createNativeQuery(java.lang.String) */
	@Override
	public final Query createNativeQuery(String sqlString) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
	 * java.lang.Class)
	 */
	@Override
	public final Query createNativeQuery(String sqlString, Class resultClass) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public final Query createNativeQuery(String sqlString,
			String resultSetMapping) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.EntityManager#createQuery(java.lang.String) */
	@Override
	public final Query createQuery(String ejbqlString) {
		return new LuceneQuery(this, metadataManager, ejbqlString);
	}

	/* @see javax.persistence.EntityManager#flush() */
	@Override
	public final void flush() {
		// always flushed to cassandra anyway! relax.
	}

	/* @see javax.persistence.EntityManager#getDelegate() */
	@Override
	public final Object getDelegate() {
		return null;
	}

	/* @see javax.persistence.EntityManager#getFlushMode() */
	@Override
	public final FlushModeType getFlushMode() {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see javax.persistence.EntityManager#getReference(java.lang.Class,
	 * java.lang.Object)
	 */
	@Override
	public final <T> T getReference(Class<T> entityClass, Object primaryKey) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.EntityManager#getTransaction() */
	@Override
	public final EntityTransaction getTransaction() {
		return new EntityTransactionImpl();
	}

	/* @see javax.persistence.EntityManager#isOpen() */
	@Override
	public final boolean isOpen() {
		return !closed;
	}

	/* @see javax.persistence.EntityManager#joinTransaction() */
	@Override
	public final void joinTransaction() {
		// TODO Auto-generated method stub

	}

	/*
	 * @see javax.persistence.EntityManager#lock(java.lang.Object,
	 * javax.persistence.LockModeType)
	 */
	@Override
	public final void lock(Object entity, LockModeType lockMode) {
		throw new NotImplementedException("TODO");
	}

	/* @see javax.persistence.EntityManager#refresh(java.lang.Object) */
	@Override
	public final void refresh(Object entity) {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * javax.persistence.EntityManager#setFlushMode(javax.persistence.FlushModeType
	 * )
	 */
	/* @see javax.persistence.EntityManager#setFlushMode(javax.persistence.FlushModeType) */
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
	protected final MetadataManager getMetadataManager() {
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

	/**
	 * Gets the session.
	 * 
	 * @return the session
	 */
	protected EntityManagerSession getSession() {
		return session;
	}

	/**
	 * Gets the entity resolver.
	 * 
	 * @return the reachabilityResolver
	 */
	public EntityResolver getEntityResolver() {
		return entityResolver;
	}
}
