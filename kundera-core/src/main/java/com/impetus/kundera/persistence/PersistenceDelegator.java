package com.impetus.kundera.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;
import javax.persistence.PersistenceException;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.event.EntityEventDispatcher;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

public class PersistenceDelegator
{
    private static final Log log = LogFactory.getLog(PersistenceDelegator.class);

    private boolean closed = false;

    private Client client;

    private EntityManagerSession session;

    /** The event dispatcher. */
    private EntityEventDispatcher eventDispatcher;

    public PersistenceDelegator(Client client, EntityManagerSession session)
    {
        super();
        this.client = client;
        this.session = session;
        eventDispatcher = new EntityEventDispatcher();
    }

    private Client getClient()
    {
        // TODO change this method to return the client as per entity in case of
        // polyglot persistence
        return client;
    }

    private EntityManagerSession getSession()
    {
        return session;
    }

    private EntityEventDispatcher getEventDispatcher()
    {
        return eventDispatcher;
    }

    public void persist(Object e)
    {
        try
        {
            List<EnhancedEntity> reachableEntities = EntityResolver.resolve(getPersistenceUnit(), e,
                    CascadeType.PERSIST);

            for (EnhancedEntity enhancedEntity : reachableEntities)
            {
                log.debug("Persisting entity : " + enhancedEntity.getEntity().getClass());

                EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(),
                        enhancedEntity.getEntity().getClass());

                // TODO: throw EntityExistsException if already exists

                // fire pre-persist events
                getEventDispatcher().fireEventListeners(entityMetadata, enhancedEntity, PrePersist.class);

                // Persist data into data-store
                client.persist(enhancedEntity);

                // Store entity into session
                session.store(enhancedEntity.getId(), enhancedEntity.getEntity());

                // fire post-persist events
                getEventDispatcher().fireEventListeners(entityMetadata, enhancedEntity, PostPersist.class);

                log.debug("Data persisted successfully for entity : " + enhancedEntity.getEntity().getClass());
            }
        }
        catch (Exception exp)
        {
            exp.printStackTrace();
            throw new PersistenceException(exp);
        }
    }

    public <E> E find(Class<E> entityClass, Object primaryKey)
    {
        try
        {
            // Look up in session first
            E e = getSession().lookup(entityClass, primaryKey);

            if (null != e)
            {
                log.debug(entityClass.getName() + "_" + primaryKey + " is loaded from cache!");
                return e;
            }

            // Fetch top level entity (including embedded objects)
            EnhancedEntity enhancedEntity = (EnhancedEntity) getClient().find(entityClass, primaryKey.toString());

            if (enhancedEntity == null)
            {
                return null;
            }

            E entity = (E) enhancedEntity.getEntity();

            EntityMetadata entityMetadata = KunderaMetadataManager.getMetamodel(getPersistenceUnit())
                    .getEntityMetadata(entityClass);
            if (entity != null)
            {
                boolean isCacheableToL2 = entityMetadata.isCacheable();
                getSession().store(primaryKey, entity, isCacheableToL2);

            }

            // Fetch relationship entities and set into top level entity
            Map<String, Set<String>> foreignKeysMap = enhancedEntity.getForeignKeysMap();
            List<Relation> relations = entityMetadata.getRelations();

            // Determine which client to use depending upon persistence unit
            // name set in relation metadata
            for (Relation relation : relations)
            {
                Set<String> foreignKeysSet = foreignKeysMap.get(relation.getProperty().getName());
                if (foreignKeysSet != null && !foreignKeysSet.isEmpty())
                {
                    populateForeignEntities(entity, relation, foreignKeysSet);
                }

            }

            return entity;
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            throw new PersistenceException(exception);
        }
    }

    public <E> E merge(E e)
    {
        try
        {
            List<EnhancedEntity> reachableEntities = EntityResolver.resolve(getPersistenceUnit(), e, CascadeType.MERGE);

            // save each one
            for (EnhancedEntity o : reachableEntities)
            {
                log.debug("Merging Entity : " + o);

                EntityMetadata m = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), o.getEntity()
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

    public void remove(Object e)
    {
        try
        {
            List<EnhancedEntity> reachableEntities = EntityResolver
                    .resolve(getPersistenceUnit(), e, CascadeType.REMOVE);

            // remove each one
            for (EnhancedEntity enhancedEntity : reachableEntities)
            {
                log.debug("Removing Entity : " + enhancedEntity);

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

    private <E> void populateForeignEntities(E entity, Relation relation, Set<String> foreignKeysSet) throws Exception
    {
        log.debug("Populating foreign entities for " + entity.getClass().getName());

        // foreignEntityClass
        Class<?> foreignEntityClass = relation.getTargetEntity();

        if (relation.isUnary())
        {
            // there is just one target object
            String foreignKey = Iterables.get(foreignKeysSet, 0);

            Object foreignObject = getForeignEntityOrProxy(relation, foreignEntityClass, foreignKey);

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObject);
        }
        else if (relation.isCollection())
        {
            // there could be multiple target objects Cast to
            // Collection
            Collection<Object> foreignObjects = null;
            if (relation.getPropertyType().equals(Set.class))
            {
                foreignObjects = new HashSet<Object>();
            }
            else if (relation.getPropertyType().equals(List.class))
            {
                foreignObjects = new ArrayList<Object>();
            }

            // Iterate over keys
            for (String foreignKey : foreignKeysSet)
            {
                Object foreignObject = getForeignEntityOrProxy(relation, foreignEntityClass, foreignKey);
                foreignObjects.add(foreignObject);
            }

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObjects);
        }
    }

    /**
     * Helper method to load Foreign Entity/Proxy
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
    private Object getForeignEntityOrProxy(Relation relation, Class<?> foreignEntityClass, String foreignKey)
            throws Exception
    {
        Object foreignObject;
        if (relation.getFetchType().equals(FetchType.EAGER))
        {

            // load target eagerly!
            foreignObject = find(foreignEntityClass, foreignKey);
        }
        else
        {
            // load target lazily!
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(),
                    foreignEntityClass);

            String entityName = foreignEntityClass.getName() + "_" + foreignKey + "#"
                    + relation.getProperty().getName();

            foreignObject = KunderaMetadataManager.getLazyInitializerFactory().getProxy(entityName, foreignEntityClass,
                    entityMetadata.getReadIdentifierMethod(), entityMetadata.getWriteIdentifierMethod(), foreignKey,
                    this);
        }
        return foreignObject;
    }

    private String getPersistenceUnit()
    {
        return client.getPersistenceUnit();
    }

    public final boolean isOpen()
    {
        return !closed;
    }

    public final void close()
    {
        eventDispatcher = null;
        closed = true;
    }

}
