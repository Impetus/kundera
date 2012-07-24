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

import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceContextType;
import javax.persistence.Query;
import javax.persistence.TransactionRequiredException;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.transaction.UserTransaction;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.context.CacheBase;
import com.impetus.kundera.persistence.context.FlushManager;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.persistence.jta.KunderaJTAUserTransaction;
import com.impetus.kundera.utils.ObjectUtils;

/**
 * The Class EntityManagerImpl.
 * 
 * @author animesh.kumar
 */
public class EntityManagerImpl implements EntityManager, EntityTransaction, ResourceManager
{

    /** The Constant log. */
    private static Log logger = LogFactory.getLog(EntityManagerImpl.class);

    /** The factory. */
    private EntityManagerFactory factory;

    /** The closed. */
    private boolean closed = false;

    /** Flush mode for this EM, default is AUTO. */
    FlushModeType flushMode = FlushModeType.AUTO;

    /** The session. */
    private EntityManagerSession session;

    /** Properties provided by user at the time of EntityManager Creation. */
    private Map<String, Object> properties;

    /** Properties provided by user at the time of EntityManager Creation. */
    private PersistenceDelegator persistenceDelegator;

    /** Persistence Context Type (Transaction/ Extended) */
    private PersistenceContextType persistenceContextType;

    /** Transaction Type (JTA/ RESOURCE_LOCAL) */
    private PersistenceUnitTransactionType transactionType;

    private PersistenceCache persistenceCache;

    FlushManager flushStackManager;

    UserTransaction utx;

    /**
     * Instantiates a new entity manager impl.
     * 
     * @param factory
     *            the factory
     */
    public EntityManagerImpl(EntityManagerFactory factory, PersistenceUnitTransactionType transactionType,
            PersistenceContextType persistenceContextType)
    {
        this.factory = factory;
        logger.debug("Creating EntityManager for persistence unit : " + getPersistenceUnit());
        session = new EntityManagerSession((Cache) factory.getCache());
        persistenceCache = new PersistenceCache();
        persistenceCache.setPersistenceContextType(persistenceContextType);

        persistenceDelegator = new PersistenceDelegator(session, persistenceCache);

        for (String pu : ((EntityManagerFactoryImpl) this.factory).getPersistenceUnits())
        {
            persistenceDelegator.loadClient(pu);
        }
        this.persistenceContextType = persistenceContextType;
        this.transactionType = transactionType;

        logger.debug("Created EntityManager for persistence unit : " + getPersistenceUnit());
    }

    private void onLookUp(PersistenceUnitTransactionType transactionType)
    {
        if (transactionType != null && transactionType.equals(PersistenceUnitTransactionType.JTA))
        {
            Context ctx;
            try
            {

                ctx = new InitialContext();

                utx = (KunderaJTAUserTransaction) ctx.lookup("java:comp/UserTransaction");

                if (utx == null)
                {
                    throw new KunderaException(
                            "Lookup for UserTransaction returning null for :{java:comp/UserTransaction}");
                }
                if (!(utx instanceof KunderaJTAUserTransaction))
                {

                    throw new KunderaException("Please bind [" + KunderaJTAUserTransaction.class.getName()
                            + "] for :{java:comp/UserTransaction} lookup" + utx.getClass());

                }

                this.setFlushMode(FlushModeType.COMMIT);
                ((KunderaJTAUserTransaction) utx).setImplementor(this);
            }
            catch (NamingException e)
            {
                logger.error("Error during initialization of entity manager, Caused by:" + e.getMessage());
                throw new KunderaException(e);
            }

        }
    }

    /**
     * Instantiates a new entity manager impl.
     * 
     * @param factory
     *            the factory
     * @param properties
     *            the properties
     */
    public EntityManagerImpl(EntityManagerFactory factory, Map properties,
            PersistenceUnitTransactionType transactionType, PersistenceContextType persistenceContextType)
    {
        this(factory, transactionType, persistenceContextType);
        this.properties = properties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public final <E> E find(Class<E> entityClass, Object primaryKey)
    {
        checkClosed();
        checkTransactionNeeded();
        // TODO Check for validity also as per JPA
        if (primaryKey == null)
        {
            throw new IllegalArgumentException("PrimaryKey value must not be null for object you want to find.");
        }

        E e = getPersistenceDelegator().find(entityClass, primaryKey);

        if (e == null)
            return null;

        // Set this returned entity as head node if applicable
        String nodeId = ObjectGraphUtils.getNodeId(primaryKey, entityClass);
        CacheBase mainCache = getPersistenceDelegator().getPersistenceCache().getMainCache();
        Node node = mainCache.getNodeFromCache(nodeId);
        if (node != null && node.getParents() == null && !mainCache.getHeadNodes().contains(node))
        {
            mainCache.addHeadNode(node);
        }

        // Return a deep copy of this entity
        return (E) ObjectUtils.deepCopy((Object) e);
    }

    @Override
    public final void remove(Object e)
    {
        checkClosed();
        checkTransactionNeeded();

        // TODO Check for validity also as per JPA
        if (e == null)
        {
            throw new IllegalArgumentException("Entity to be removed must not be null.");
        }

        try
        {
            getPersistenceDelegator().remove(e);
        }
        catch (Exception ex)
        {
            // on rollback.
            getPersistenceDelegator().rollback();
            throw new KunderaException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#merge(java.lang.Object)
     */
    @Override
    public final <E> E merge(E e)
    {
        checkClosed();
        checkTransactionNeeded();

        if (e == null)
        {
            getPersistenceDelegator().rollback();
            throw new IllegalArgumentException("Entity to be merged must not be null.");
        }

        try
        {
            return getPersistenceDelegator().merge(e);
        }
        catch (Exception ex)
        {
            // on Rollback
            getPersistenceDelegator().rollback();

            throw new KunderaException(ex);
        }

    }

    @Override
    public final void persist(Object e)
    {
        checkClosed();
        checkTransactionNeeded();

        if (e == null)
        {
            getPersistenceDelegator().rollback();
            throw new IllegalArgumentException("Entity to be persisted must not be null.");
        }

        try
        {
            getPersistenceDelegator().persist(e);
        }
        catch (Exception ex)
        {
            // onRollBack.
            getPersistenceDelegator().rollback();
            throw new KunderaException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#clear()
     */
    @Override
    public final void clear()
    {
        checkClosed();
        session.clear();

        // TODO Do we need a client and persistenceDelegator close here?
        if (!PersistenceUnitTransactionType.JTA.equals(transactionType))
        {
            persistenceDelegator.clear();
        }

    }

    @Override
    public final void close()
    {
        checkClosed();
        session.clear();
        session = null;
        persistenceDelegator.close();

        if (!PersistenceUnitTransactionType.JTA.equals(transactionType))
        {
            persistenceDelegator.clear();
        }
        closed = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#contains(java.lang.Object)
     */
    @Override
    public final boolean contains(Object entity)
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createQuery(java.lang.String)
     */
    @Override
    public final Query createQuery(String query)
    {
        checkTransactionNeeded();
        return persistenceDelegator.createQuery(query);
    }

    @Override
    public final void flush()
    {
        checkClosed();
        doCommit();
        // persistenceDelegator.flush();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getDelegate()
     */
    @Override
    public final Object getDelegate()
    {
        return persistenceDelegator.getDelegate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNamedQuery(java.lang.String)
     */
    @Override
    public final Query createNamedQuery(String name)
    {
        checkTransactionNeeded();
        return persistenceDelegator.createQuery(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String)
     */
    @Override
    public final Query createNativeQuery(String sqlString)
    {
        throw new NotImplementedException("Please use createNativeQuery(String sqlString, Class resultClass) instead. ");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public final Query createNativeQuery(String sqlString, Class resultClass)
    {
        checkTransactionNeeded();
        // Add to meta data first.
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        if (appMetadata.getQuery(sqlString) == null)
        {
            appMetadata.addQueryToCollection(sqlString, sqlString, true, resultClass);
        }

        return persistenceDelegator.createQuery(sqlString);

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNativeQuery(java.lang.String,
     * java.lang.String)
     */
    @Override
    public final Query createNativeQuery(String sqlString, String resultSetMapping)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getReference(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public final <T> T getReference(Class<T> entityClass, Object primaryKey)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final FlushModeType getFlushMode()
    {
        return this.flushMode;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getTransaction()
     */
    @Override
    public final EntityTransaction getTransaction()
    {
        if (this.transactionType == PersistenceUnitTransactionType.JTA)
        {
            throw new IllegalStateException("A JTA EntityManager cannot use getTransaction()");
        }
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#joinTransaction()
     */
    @Override
    public final void joinTransaction()
    {
        if (utx != null)
        {
            return;
        }
        else
        {
            throw new TransactionRequiredException("No transaction in progress");
        }

        // throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#lock(java.lang.Object,
     * javax.persistence.LockModeType)
     */
    @Override
    public final void lock(Object entity, LockModeType lockMode)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#refresh(java.lang.Object)
     */
    @Override
    public final void refresh(Object entity)
    {
        checkTransactionNeeded();
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#find(java.lang.Class,
     * java.lang.Object, javax.persistence.LockModeType)
     */
    @Override
    public <T> T find(Class<T> paramClass, Object paramObject, LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#lock(java.lang.Object,
     * javax.persistence.LockModeType, java.util.Map)
     */
    @Override
    public void lock(Object paramObject, LockModeType paramLockModeType, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#refresh(java.lang.Object,
     * java.util.Map)
     */
    @Override
    public void refresh(Object paramObject, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#refresh(java.lang.Object,
     * javax.persistence.LockModeType)
     */
    @Override
    public void refresh(Object paramObject, LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#refresh(java.lang.Object,
     * javax.persistence.LockModeType, java.util.Map)
     */
    @Override
    public void refresh(Object paramObject, LockModeType paramLockModeType, Map<String, Object> paramMap)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#detach(java.lang.Object)
     */
    @Override
    public void detach(Object paramObject)
    {
        throw new NotImplementedException("TODO");

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getLockMode(java.lang.Object)
     */
    @Override
    public LockModeType getLockMode(Object paramObject)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#setProperty(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void setProperty(String paramString, Object paramObject)
    {
        throw new NotImplementedException("TODO");

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.EntityManager#createQuery(javax.persistence.criteria
     * .CriteriaQuery)
     */
    @Override
    public <T> TypedQuery<T> createQuery(CriteriaQuery<T> paramCriteriaQuery)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createQuery(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T> TypedQuery<T> createQuery(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#createNamedQuery(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T> TypedQuery<T> createNamedQuery(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public final void setFlushMode(FlushModeType flushMode)
    {
        this.flushMode = flushMode;
        persistenceDelegator.setFlushMode(flushMode);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getProperties()
     */
    @Override
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getEntityManagerFactory()
     */
    @Override
    public EntityManagerFactory getEntityManagerFactory()
    {
        return factory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getCriteriaBuilder()
     */
    @Override
    public CriteriaBuilder getCriteriaBuilder()
    {
        return factory.getCriteriaBuilder();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#getMetamodel()
     */
    @Override
    public Metamodel getMetamodel()
    {
        return factory.getMetamodel();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#isOpen()
     */
    @Override
    public final boolean isOpen()
    {
        return !closed;
    }

    /**
     * Check closed.
     */
    private void checkClosed()
    {
        if (!isOpen())
        {
            throw new IllegalStateException("EntityManager has already been closed.");
        }
    }

    private void checkTransactionNeeded()
    {
        onLookUp(transactionType);

        if ((this.persistenceContextType != PersistenceContextType.TRANSACTION)
                || (persistenceDelegator.isTransactionInProgress()))
            return;

        throw new TransactionRequiredException(
                "no transaction is in progress for a TRANSACTION type persistence context");
    }

    /**
     * Returns Persistence unit (or comma separated units) associated with EMF.
     * 
     * @return the persistence unit
     */
    private String getPersistenceUnit()
    {
        return (String) this.factory.getProperties().get(Constants.PERSISTENCE_UNIT_NAME);
    }

    /**
     * Gets the session.
     * 
     * @return the session
     */
    private EntityManagerSession getSession()
    {
        return session;
    }

    /**
     * Gets the persistence delegator.
     * 
     * @return the persistence delegator
     */
    private PersistenceDelegator getPersistenceDelegator()
    {
        return persistenceDelegator;
    }

    /**
     * @return the persistenceContextType
     */
    public PersistenceContextType getPersistenceContextType()
    {
        return persistenceContextType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.EntityImplementor#doCommit()
     */
    @Override
    public void doCommit()
    {

        checkClosed();
        persistenceDelegator.commit();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.EntityImplementor#doRollback()
     */
    @Override
    public void doRollback()
    {
        checkClosed();
        persistenceDelegator.rollback();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#find(java.lang.Class,
     * java.lang.Object, java.util.Map)
     */
    @Override
    public <T> T find(Class<T> arg0, Object arg1, Map<String, Object> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.EntityManager#find(java.lang.Class,
     * java.lang.Object, javax.persistence.LockModeType, java.util.Map)
     */
    @Override
    public <T> T find(Class<T> arg0, Object arg1, LockModeType arg2, Map<String, Object> arg3)
    {
        // TODO Auto-generated method stub
        return null;
    }

    // ///////////////////////////////////////////////////////////////////////
    /** Methods from {@link EntityTransaction} interface */
    // ///////////////////////////////////////////////////////////////////////

    @Override
    public void begin()
    {
        persistenceDelegator.begin();
    }

    @Override
    public void commit()
    {
        doCommit();
    }

    @Override
    public boolean getRollbackOnly()
    {
        if (!isActive())
        {
            throw new IllegalStateException("No active transaction found");
        }
        return persistenceDelegator.getRollbackOnly();
    }

    @Override
    public void setRollbackOnly()
    {
        persistenceDelegator.setRollbackOnly();
    }

    @Override
    public boolean isActive()
    {
        return isOpen() && persistenceDelegator.isActive();
    }

    @Override
    public void rollback()
    {
        doRollback();
    }
}
