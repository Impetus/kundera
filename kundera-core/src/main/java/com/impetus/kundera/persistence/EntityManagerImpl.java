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

import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
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
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.event.EntityEventDispatcher;
import com.impetus.kundera.proxy.EnhancedEntity;

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

    // TODO To move the client to persistence delegator
    /** The client. */
    private Client client;

    /** The session. */
    private EntityManagerSession session;

    /** The event dispatcher. */
    private EntityEventDispatcher eventDispatcher;

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
        session = new EntityManagerSession(this);
        eventDispatcher = new EntityEventDispatcher();
        client = ClientResolver.getClient(getPersistenceUnit());
        persistenceDelegator = new PersistenceDelegator(client, session);
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

        try
        {

            List<EnhancedEntity> reachableEntities = EntityResolver
                    .resolve(getPersistenceUnit(), e, CascadeType.REMOVE);

            // remove each one
            for (EnhancedEntity enhancedEntity : reachableEntities)
            {
                logger.debug("Removing Entity : " + enhancedEntity);

                // fire PreRemove events
                getEventDispatcher().fireEventListeners(
                        KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity.getEntity()
                                .getClass()), enhancedEntity, PreRemove.class);

                session.remove(enhancedEntity.getEntity().getClass(), enhancedEntity.getId());

                client.delete(enhancedEntity);

                // fire PostRemove events
                getEventDispatcher().fireEventListeners(
                        KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity.getEntity()
                                .getClass()), enhancedEntity, PostRemove.class);
            }
        }
        catch (Exception exp)
        {
            throw new PersistenceException(exp);
        }
    }

    @Override
    public final <E> E merge(E e)
    {
        checkClosed();
        if (e == null)
        {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        try
        {
            List<EnhancedEntity> reachableEntities = EntityResolver.resolve(getPersistenceUnit(), e, CascadeType.MERGE);

            // save each one
            for (EnhancedEntity o : reachableEntities)
            {
                logger.debug("Merging Entity : " + o);

                EntityMetadata m = ((MetamodelImpl) this.factory.getMetamodel()).getEntityMetadata(o.getEntity()
                        .getClass());

                // TODO: throw OptisticLockException if wrong version and
                // optimistic locking enabled

                // fire PreUpdate events
                getEventDispatcher().fireEventListeners(m, o, PreUpdate.class);

                client.persist(o);

                // fire PreUpdate events
                getEventDispatcher().fireEventListeners(m, o, PostUpdate.class);
            }
        }
        catch (Exception exp)
        {
            throw new PersistenceException(exp);
        }

        return e;
    }

    @Override
    public final void persist(Object e)
    {
        checkClosed();
        if (e == null)
        {
            throw new IllegalArgumentException("Entity must not be null.");
        }

        try
        {
            List<EnhancedEntity> reachableEntities = EntityResolver.resolve(getPersistenceUnit(), e,
                    CascadeType.PERSIST);

            for (EnhancedEntity enhancedEntity : reachableEntities)
            {
                logger.debug("Persisting entity : " + enhancedEntity.getEntity().getClass());

                EntityMetadata entityMetadata = ((MetamodelImpl) this.factory.getMetamodel())
                        .getEntityMetadata(enhancedEntity.getEntity().getClass());

                // TODO: throw EntityExistsException if already exists

                // fire pre-persist events
                getEventDispatcher().fireEventListeners(entityMetadata, enhancedEntity, PrePersist.class);
                
                //Persist data into data-store
                client.persist(enhancedEntity);
                
                //Store entity into session
                session.store(enhancedEntity.getId(), enhancedEntity.getEntity());                

                // fire post-persist events
                getEventDispatcher().fireEventListeners(entityMetadata, enhancedEntity, PostPersist.class);

                logger.debug("Data persisted successfully for entity : " + enhancedEntity.getEntity().getClass());
            }
        }
        catch (Exception exp)
        {
            exp.printStackTrace();
            throw new PersistenceException(exp);
        }
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
        eventDispatcher = null;
        persistenceDelegator.close();
        client.close();
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
        return this.client.createQuery(query);
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

    private final Client getClient()
    {
        return client;
    }

    private String getPersistenceUnit()
    {
        return (String) this.factory.getProperties().get(Constants.PERSISTENCE_UNIT_NAME);
    }

    private EntityManagerSession getSession()
    {
        return session;
    }

    private EntityEventDispatcher getEventDispatcher()
    {
        return eventDispatcher;
    }

    private PersistenceDelegator getPersistenceDelegator()
    {
        return persistenceDelegator;
    }

}
