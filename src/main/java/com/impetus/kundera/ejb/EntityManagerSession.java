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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.cache.Cache;

/**
 * The Class EntityManagerCache.
 */
public class EntityManagerSession {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerSession.class);

    /** cache is used to store objects retrieved in this EntityManager session. */
    private Map<Object, Object> sessionCache;
    
    /** The em. */
    private EntityManagerImpl em;
    
    /**
	 * Instantiates a new entity manager cache.
	 * 
	 * @param em
	 *            the em
	 */
    public EntityManagerSession (EntityManagerImpl em) {
    	this.em = em;
    	this.sessionCache = new ConcurrentHashMap<Object, Object>();
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
	protected <T> T lookup (Class<T> entityClass, Object id) {
        String key = cacheKey(entityClass, id);
        log.debug("Reading from L1 >> " + key);
        T o = (T) sessionCache.get(key);

        // go to second-level cache
        if (o == null) {
        	log.debug("Reading from L2 >> " + key);
            Cache c = em.getFactory().getCache(entityClass);
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
	 * Store in L1 only.
	 * 
	 * @param id
	 *            the id
	 * @param entity
	 *            the entity
	 */
    protected void store (Object id, Object entity) {
    	store (id, entity, Boolean.FALSE);
    }
    
    /**
	 * Save to cache.
	 * 
	 * @param id
	 *            the id
	 * @param entity
	 *            the entity
	 * @param spillOverToL2
	 *            the spill over to l2
	 */
    protected void store (Object id, Object entity, boolean spillOverToL2) {
        String key = cacheKey(entity.getClass(), id);
        log.debug("Writing to L1 >> " + key);
        sessionCache.put(key, entity);
        
		if (spillOverToL2) {
			log.debug("Writing to L2 >>" + key);
			// save to second level cache
			Cache c = em.getFactory().getCache(entity.getClass());
			if (c != null) {
				c.put(key, entity);
			}
		}
    }

    /**
	 * Removes the.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param entityClass
	 *            the entity class
	 * @param id
	 *            the id
	 */
    protected <T> void remove (Class<T> entityClass, Object id) {
    	remove(entityClass, id, Boolean.FALSE);
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
	 * @param spillOverToL2
	 *            the spill over to l2
	 */
    protected <T> void remove (Class<T> entityClass, Object id, boolean spillOverToL2) {
        String key = cacheKey(entityClass, id);
        log.debug("Removing from L1 >> " + key);
        Object o = sessionCache.remove(key);
        
		if (spillOverToL2) {
			log.debug("Removing from L2 >> " + key);
			Cache c = em.getFactory().getCache(entityClass);
			if (c != null) {
				Object o2 = c.remove(key);
				if (o == null) {
					o = o2;
				}
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
    
    public final void clear() {
    	sessionCache = new ConcurrentHashMap<Object, Object>();
    }
}
