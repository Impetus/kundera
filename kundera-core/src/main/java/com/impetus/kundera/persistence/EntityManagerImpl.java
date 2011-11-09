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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.Cache;

/**
 * The Class EntityManagerImpl.
 * 
 * @author animesh.kumar
 */
public class EntityManagerImpl implements EntityManager
{

    /** The Constant log. */
    private static Log logger = LogFactory.getLog(EntityManagerFactoryImpl.class);

    /** The factory. */
    private EntityManagerFactory factory;

    /** The closed. */
    private boolean closed = false;

    /** The session. */
    private EntityManagerSession session;

    /** Properties provided by user at the time of EntityManager Creation. */
    private Map<String, Object> properties;

    /** Properties provided by user at the time of EntityManager Creation. */
    private PersistenceDelegator persistenceDelegator;

    /**
     * Instantiates a new entity manager impl.
     * 
     * @param factory
     *            the factory
     * @param client
     *            the client
     */
    public EntityManagerImpl(EntityManagerFactory factory)
    {
        this.factory = factory;
        logger.debug("Creating EntityManager for persistence unit : " + getPersistenceUnit());
        session = new EntityManagerSession((Cache) factory.getCache());

        persistenceDelegator = new PersistenceDelegator(session, getPersistenceUnit().split(
                Constants.PERSISTENCE_UNIT_SEPARATOR));
        logger.debug("Created EntityManager for persistence unit : " + getPersistenceUnit());
    }

    /**
     * Instantiates a new entity manager impl.
     * 
     * @param factory
     *            the factory
     * @param client
     *            the client
     */
    public EntityManagerImpl(EntityManagerFactory factory, Map properties)
    {
        this(factory);
        this.properties = properties;
    }

    @Override
    public final <E> E find(Class<E> entityClass, Object primaryKey)
    {
        checkClosed();
        // TODO Check for validity also as per JPA
        if (primaryKey == null)
        {
            throw new IllegalArgumentException("primaryKey value must not be null.");
        }

        return getPersistenceDelegator().find(entityClass, primaryKey);
    }

    @Override
    public final void remove(Object e)
    {
        checkClosed();
        // TODO Check for validity also as per JPA
        if (e == null)
        {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        getPersistenceDelegator().remove(e);
    }

    @Override
    public final <E> E merge(E e)
    {
        checkClosed();
        if (e == null)
        {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        return getPersistenceDelegator().merge(e);
    }

    @Override
    public final void persist(Object e)
    {
        checkClosed();
        if (e == null)
        {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        getPersistenceDelegator().persist(e);
    }

    @Override
    public final void clear()
    {
        checkClosed();
        session.clear();
        // TODO Do we need a client and persistenceDelegator close here?
    }

    @Override
    public final void close()
    {
        checkClosed();
        session = null;
        persistenceDelegator.close();
        closed = true;
    }

    @Override
    public final boolean contains(Object entity)
    {
        return false;
    }

    @Override
    public final Query createQuery(String query)
    {
        return persistenceDelegator.createQuery(query);
    }

    @Override
    public final void flush()
    {
        // always flushed to cassandra anyway! relax.
    }

    @Override
    public final Object getDelegate()
    {
        return null;
    }

    @Override
    public final Query createNamedQuery(String name)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final Query createNativeQuery(String sqlString)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final Query createNativeQuery(String sqlString, Class resultClass)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final Query createNativeQuery(String sqlString, String resultSetMapping)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final <T> T getReference(Class<T> entityClass, Object primaryKey)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final FlushModeType getFlushMode()
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final EntityTransaction getTransaction()
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final void joinTransaction()
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final void lock(Object entity, LockModeType lockMode)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final void refresh(Object entity)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> T find(Class<T> paramClass, Object paramObject, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> T find(Class<T> paramClass, Object paramObject, LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> T find(Class<T> paramClass, Object paramObject, LockModeType paramLockModeType,
            Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void lock(Object paramObject, LockModeType paramLockModeType, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void refresh(Object paramObject, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");

    }

    @Override
    public void refresh(Object paramObject, LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");

    }

    @Override
    public void refresh(Object paramObject, LockModeType paramLockModeType, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void detach(Object paramObject)
    {
        throw new NotImplementedException("TODO");

    }

    @Override
    public LockModeType getLockMode(Object paramObject)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void setProperty(String paramString, Object paramObject)
    {
        throw new NotImplementedException("TODO");

    }

    @Override
    public <T> TypedQuery<T> createQuery(CriteriaQuery<T> paramCriteriaQuery)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> TypedQuery<T> createQuery(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> TypedQuery<T> createNamedQuery(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final void setFlushMode(FlushModeType flushMode)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    @Override
    public EntityManagerFactory getEntityManagerFactory()
    {
        return factory;
    }

    @Override
    public CriteriaBuilder getCriteriaBuilder()
    {
        return factory.getCriteriaBuilder();
    }

    @Override
    public Metamodel getMetamodel()
    {
        return factory.getMetamodel();
    }

    @Override
    public final boolean isOpen()
    {
        return !closed;
    }

    private void checkClosed()
    {
        if (!isOpen())
        {
            throw new IllegalStateException("EntityManager has been closed.");
        }
    }

    /**
     * Returns Persistence unit (or comma separated units) associated with EMF
     * 
     * @return
     */
    private String getPersistenceUnit()
    {
        return (String) this.factory.getProperties().get(Constants.PERSISTENCE_UNIT_NAME);
    }

    private EntityManagerSession getSession()
    {
        return session;
    }

    private PersistenceDelegator getPersistenceDelegator()
    {
        return persistenceDelegator;
    }

}
