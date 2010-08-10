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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.CascadeType;
import javax.persistence.EntityTransaction;
import javax.persistence.FetchType;
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
import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.db.DataManager;
import com.impetus.kundera.ejb.event.CallbackMethod;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataManager;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
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

    /** cache is used to store objects retrieved in this EntityManager session. */
    private Map<Object, Object> sessionCache;

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
     */
    public EntityManagerImpl(EntityManagerFactoryImpl factory, CassandraClient client) {
        this.factory = factory;
        this.metadataManager = factory.getMetadataManager();
        this.client = client;
        this.sessionCache = new ConcurrentHashMap<Object, Object>();
        this.persistenceUnitName = factory.getPersistenceUnitName();
        dataManager = new DataManager(this);
        indexManager = new IndexManager(this);
    }

    /**
	 * Gets the factory.
	 * 
	 * @return the factory
	 */
	public EntityManagerFactoryImpl getFactory() {
		return factory;
	}

    /* @see javax.persistence.EntityManager#find(java.lang.Class, java.lang.Object) */
	@Override
    public final <E> E find(Class<E> entityClass, Object primaryKey) {
        if (closed) {
            throw new PersistenceException("EntityManager already closed.");
        }
        if (primaryKey == null) {
            throw new IllegalArgumentException("primaryKey value must not be null.");
        }
        
        E e = null;
        e = findInCache(entityClass, primaryKey);
        if (null != e) {
            log.debug(entityClass.getName() + "_" + primaryKey + " is loaded from cache!");
            return e;
        }
        
        return immediateLoadAndCache (entityClass, primaryKey);        
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
    private <E> E immediateLoadAndCache (Class<E> entityClass, Object primaryKey) {
        try {
        	EntityMetadata m = metadataManager.getEntityMetadata(entityClass);
            E e = dataManager.find(entityClass, m, primaryKey.toString());
        	saveToCache(primaryKey, e, m.isCacheable());
            return e;
        } catch (Exception exp) {
        	exp.printStackTrace();
        	throw new PersistenceException(exp.getMessage());        	
        }        
    }

    /* @see com.impetus.kundera.CassandraEntityManager#find(java.lang.Class, java.lang.Object[]) */
    @Override
    public final <E> List<E> find(Class<E> entityClass, Object... primaryKeys) {
        if (closed) {
            throw new PersistenceException("EntityManager already closed.");
        }
        if (primaryKeys == null) {
            throw new IllegalArgumentException("primaryKey value must not be null.");
        }
        
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
    public final void remove (Object e) {
        if (e == null) {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        try {

        	// get EntityMutators
        	List<EnhancedEntity> reachableEntities = getAllReachableEntities (e, CascadeType.REMOVE);
        	
        	// save each one
        	for (EnhancedEntity o : reachableEntities) {
        		log.debug("Removing @Entity >> " + o);
        		
        		EntityMetadata m = metadataManager.getEntityMetadata(o.getEntity().getClass());

                // fire pre-persist events
                fireJPAEventListeners(m, o, PreRemove.class);
                
                removeFromCache(o.getEntity().getClass(), o.getId());
        		dataManager.remove(o, m);
        		indexManager.remove (m, o.getEntity(), o.getId());
        		
                // fire post-persist events
                fireJPAEventListeners(m, o, PostRemove.class);        	
             }
        } catch (Exception exp) {
        	exp.printStackTrace();
            throw new PersistenceException(exp.getMessage());
        }
    }

    /* @see javax.persistence.EntityManager#merge(java.lang.Object) */
    @Override
    public final <E> E merge (E e) {
        if (e == null) {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        try {

        	// get EntityMutators
        	List<EnhancedEntity> reachableEntities = getAllReachableEntities (e, CascadeType.MERGE);
        	
        	// save each one
        	for (EnhancedEntity o : reachableEntities) {
        		log.debug("Merging @Entity >> " + o);
        		
        		EntityMetadata metadata = metadataManager.getEntityMetadata(o.getEntity().getClass());

                // fire pre-persist events
                fireJPAEventListeners(metadata, o, PreUpdate.class);

        		dataManager.merge(o, metadata);
        		indexManager.update(metadata, o.getEntity());
        		
                // fire post-persist events
                fireJPAEventListeners(metadata, o, PostUpdate.class);        	
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

        	// get EntityMutators
        	List<EnhancedEntity> reachableEntities = getAllReachableEntities (e, CascadeType.PERSIST);
        	
        	// save each one
        	for (EnhancedEntity o : reachableEntities) {
        		log.debug("Persisting @Entity >> " + o);
        		
        		EntityMetadata metadata = metadataManager.getEntityMetadata(o.getEntity().getClass());

                // fire pre-persist events
                fireJPAEventListeners(metadata, o, PrePersist.class);

        		dataManager.persist(o, metadata);
        		indexManager.write(metadata, o.getEntity());
        		
                // fire post-persist events
                fireJPAEventListeners(metadata, o, PostPersist.class);        	
             }
        } catch (Exception exp) {
            throw new PersistenceException(exp.getMessage());
        }
    }

    /* @see javax.persistence.EntityManager#clear() */
    @Override
    public final void clear() {
        checkClosed();
        // this is really only useful with transactions
        if (sessionCache != null) {
            sessionCache = new ConcurrentHashMap<Object, Object>();
        }
    }

    /* @see javax.persistence.EntityManager#close() */
    @Override
    public final void close() {
        closed = true;
        sessionCache = null;
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

    /* @see javax.persistence.EntityManager#createNativeQuery(java.lang.String, java.lang.Class) */
    @Override
    public final Query createNativeQuery(String sqlString, Class resultClass) {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.EntityManager#createNativeQuery(java.lang.String, java.lang.String) */
    @Override
    public final Query createNativeQuery(String sqlString, String resultSetMapping) {
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

    /* @see javax.persistence.EntityManager#getReference(java.lang.Class, java.lang.Object) */
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

    /* @see javax.persistence.EntityManager#lock(java.lang.Object, javax.persistence.LockModeType) */
    @Override
    public final void lock(Object entity, LockModeType lockMode) {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.EntityManager#refresh(java.lang.Object) */
    @Override
    public final void refresh(Object entity) {
        throw new NotImplementedException("TODO");
    }

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
     * Fire jpa event listeners.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param event
     *            the event
     * 
     * @throws Exception
     *             the exception
     */
    private void fireJPAEventListeners(EntityMetadata metadata, Object entity, Class<?> event) throws Exception {
        // handle external listeners first
        List<? extends CallbackMethod> callBackMethods = metadata.getCallbackMethods(event);
        if (null != callBackMethods) {
        	log.debug("Callback >> " + event.getSimpleName() + " on " + metadata.getEntityClazz().getName());
        	for (CallbackMethod callback : callBackMethods) {
                log.debug("Firing >> " + callback);
                try {
                    callback.invoke(entity);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
    }
    

	// helper methods for session-level-cache
    /**
	 * Gets the session cache.
	 * 
	 * @return the session cache
	 */
	public Map<Object, Object> getSessionCache () {
    	return sessionCache;
    }
    
    /**
	 * Find in cache.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param entityClass
	 *            the entity class
	 * @param id
	 *            the id
	 * @return the t
	 */
    @SuppressWarnings("unchecked")
	private <T> T findInCache(Class<T> entityClass, Object id) {
        String key = cacheKey(entityClass, id);
        log.debug("Cache >> Read >> " + key);
        T o = (T) sessionCache.get(key);

        // go to second-level cache
        if (o == null) {
            Cache c = factory.getCache(entityClass);
            if (c != null) {
                o = (T) c.get (key);
                if (o != null) {
                	log.debug("Found item in second level cache!");
                }
            }
        }
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
    	saveToCache (id, entity, false);
    }
    
    /**
	 * Save to cache.
	 * 
	 * @param id
	 *            the id
	 * @param entity
	 *            the entity
	 * @param cacheToSecondLevel
	 *            the cache to second level
	 */
    private void saveToCache(Object id, Object entity, boolean cacheToSecondLevel) {
        String key = cacheKey(entity.getClass(), id);
        log.debug("Cache >> Save >> " + key);
        sessionCache.put(key, entity);
        
		if (cacheToSecondLevel) {
			// save to second level cache
			Cache c = factory.getCache(entity.getClass());
			if (c != null) {
				c.put(key, entity);
			}
		}
    }

    /**
	 * Removes the from cache.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param entityClass
	 *            the entity class
	 * @param id
	 *            the id
	 */
    private <T> void removeFromCache(Class<T> entityClass, Object id) {
        String key = cacheKey(entityClass, id);
        log.debug("Cache >> Remove >> " + key);
        Object o = sessionCache.remove(key);
        
        Cache c = factory.getCache(entityClass);
        if (c != null) {
            Object o2 = c.remove(key);
            if(o == null) {
            	o = o2;
            }
        }
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
    
	/**
	 * Creates a list of all reachable objects from object "o", so that
	 * transaction-al lock can be acquired on all objects.
	 * 
	 * @param o
	 *            the o
	 * @param cascadeType
	 *            the cascade type
	 * @return the all reachable entities
	 */
	public List<EnhancedEntity> getAllReachableEntities (Object o,
			CascadeType cascadeType) {
		Map<String, EnhancedEntity> map = new HashMap<String, EnhancedEntity>();
		try {
			getAllReachableEntities(o, cascadeType, map);
		} catch (PropertyAccessException e) {
			throw new PersistenceException(e.getMessage());
		}
		return new ArrayList<EnhancedEntity>(map.values());
	}

	// helper method to recursively build reachable object list.
	/**
	 * Gets the all reachable entities.
	 * 
	 * @param o
	 *            the o
	 * @param cascadeType
	 *            the cascade type
	 * @param entities
	 *            the entities
	 * @return the all reachable entities
	 * @throws PropertyAccessException
	 *             the property access exception
	 */
	private void getAllReachableEntities(Object o, CascadeType cascadeType,
			Map<String, EnhancedEntity> entities)
			throws PropertyAccessException {
		
		EntityMetadata m = metadataManager.getEntityMetadata(
				o.getClass());
		
		String id = PropertyAccessorHelper.getId(o, m);

		// Ensure that @Id is set
		if (null == id || id.trim().isEmpty()) {
			throw new PersistenceException("Missing primary key >> "
					+ m.getEntityClazz().getName() + "#"
					+ m.getIdProperty().getName());
		}
		
		// dummy name, to check if this object was processed earlier.
		String uniqueEntityName = o.getClass().getName() + Constants.SEPARATOR
				+ id;

		// return if this entity has already been processed!
		if (entities.containsKey(uniqueEntityName)) {
			return;
		}
		entities.put(uniqueEntityName, null);

		// Map to hold property-name=>foreign-entity relations
		Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

		// iterate over each ToOne entities mapped to this entity.
		for (EntityMetadata.Relation relation : m.getRelations()) {

			// check for cascade
			if (!relation.getCascades().contains(CascadeType.ALL)
					&& !relation.getCascades().contains(cascadeType)) {
				continue;
			}

			// target-entity
			Class<?> targetClass = relation.getTargetEntity();
			// mapped to this property
			Field targetField = relation.getProperty();
			// is it optional?
			boolean optional = relation.isOptional();

			// read value
			Object value = PropertyAccessorHelper.getObject(o, targetField);

			// if object is not null, then proceed
			if (null != value) {
				
				if (relation.isUnary()) {
					// Now since this relationship is unary, there will be a 
					// single target object.
					String targetId = PropertyAccessorHelper.getId(value, 
										metadataManager.getEntityMetadata(
												targetClass
										));
					
					Set<String> foreignKeys = new HashSet<String>();
					foreignKeys.add(targetId);
					// put to map
					foreignKeysMap.put(targetField.getName(), foreignKeys);
					
					// get all other reachable objects from object "value"
					getAllReachableEntities(value, cascadeType, entities);

				} if (relation.isCollection()) {
					// Now since this relationship is NOT unary, there could be 
					// many target objects, so we will fetch all of them, and 
					// combine their Ids together. 

					// Value must map to Collection interface.
					Collection collection = (Collection) value;
					
					Set<String> foreignKeys = new HashSet<String>();
					
					for (Object o_ : collection) {
						String targetId = PropertyAccessorHelper.getId(o_, 
								metadataManager.getEntityMetadata(targetClass));
						
						foreignKeys.add(targetId);
						// get all other reachable objects from "o_"
						getAllReachableEntities(o_, cascadeType, entities);
					}
					foreignKeysMap.put(targetField.getName(), foreignKeys);
				}
			}

			// value is null
			else {
				// halt, if this was a non-optional property
				if (!optional) {
					throw new PersistenceException("Missing "
							+ targetClass.getName() + "." + targetField.getName());
				}
			}
		}

		// put to map
		entities.put(uniqueEntityName, factory.getEnhancedEntity(o, id, foreignKeysMap));
	}
    
	
	/**
	 * Populate foreign entities.
	 * 
	 * @param containingEntity
	 *            the containing entity
	 * @param containingEntityId
	 *            the containing entity id
	 * @param relation
	 *            the relation
	 * @param foreignKeys
	 *            the foreign keys
	 * @throws PropertyAccessException
	 *             the property access exception
	 */
	public void populateForeignEntities(Object containingEntity, String containingEntityId,
			EntityMetadata.Relation relation, String... foreignKeys)
			throws PropertyAccessException {
		
		if (null == foreignKeys || foreignKeys.length == 0) {
			return;
		}
		
		// target entity class
		Class<?> foreignEntityClass = relation.getTargetEntity();
		String foreignEntityName = foreignEntityClass.getSimpleName();

		// Eagerly Caching containing entity to avoid it's own loading, 
		// in case of target contains a reference to it. 
		saveToCache(containingEntityId, containingEntity);

		if (relation.isUnary()) {
			// there will is just one target object
			String foreignKey = foreignKeys[0];
			
			Object foreignObject = getForeignEntityOrProxy(foreignEntityName,
					foreignEntityClass, foreignKey, relation);
			
			PropertyAccessorHelper.set(containingEntity,
					relation.getProperty(), foreignObject);
		}

		else if (relation.isCollection()) {
			// there could be multiple objects

			// Cast to Collection
			Collection<Object> foreignObjects = null;
			if (relation.getPropertyType().equals(Set.class)) {
				foreignObjects = new HashSet<Object>();
			} else if (relation.getPropertyType().equals(List.class)) {
				foreignObjects = new ArrayList<Object>();
			}

			// Iterate over keys
			for (String foreignKey : foreignKeys) {
				Object foreignObject = getForeignEntityOrProxy(
						foreignEntityName, foreignEntityClass, foreignKey,
						relation);
				foreignObjects.add(foreignObject);
			}

			PropertyAccessorHelper.set(containingEntity, relation.getProperty(),
					foreignObjects);
		}
	}
	
	// Helper method to load Foreign Entity/Proxy
	/**
	 * Gets the foreign entity or proxy.
	 * 
	 * @param entityName
	 *            the entity name
	 * @param persistentClass
	 *            the persistent class
	 * @param foreignKey
	 *            the foreign key
	 * @param relation
	 *            the relation
	 * @return the foreign entity or proxy
	 */
	private Object getForeignEntityOrProxy(String entityName,
			Class<?> persistentClass, String foreignKey,
			EntityMetadata.Relation relation) {
		
		// Check in session cache!
		Object cached = findInCache(persistentClass, foreignKey);
		if (cached != null) {
			return cached;
		}
		
		
		FetchType fetch = relation.getFetchType();
		
		if (fetch.equals(FetchType.EAGER)) {
			log.debug("Eagerly loading "
					+ persistentClass.getName() + "_" + foreignKey);
			// load target eagerly!
			return immediateLoadAndCache (persistentClass, foreignKey);
		} else {
			log.debug("Proxy >> Create >> " + persistentClass.getName() + "#"
					+ relation.getProperty().getName() + "_" + foreignKey);
		
			// metadata
			EntityMetadata m = metadataManager.getEntityMetadata(persistentClass);
			
			return factory.getLazyEntity(entityName, persistentClass, m
					.getReadIdentifierMethod(), m.getWriteIdentifierMethod(), foreignKey,
					this);
		}
	}

}
