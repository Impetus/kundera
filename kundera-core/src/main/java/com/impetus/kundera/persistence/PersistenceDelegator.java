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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.FlushModeType;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.lifecycle.states.TransientState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.context.FlushStack;
import com.impetus.kundera.persistence.context.FlushManager;
import com.impetus.kundera.persistence.context.MainCache;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;
import com.impetus.kundera.persistence.event.EntityEventDispatcher;
import com.impetus.kundera.persistence.handler.impl.EntityInterceptor;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.QueryResolver;

/**
 * The Class PersistenceDelegator.
 */
public class PersistenceDelegator
{

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(PersistenceDelegator.class);

    /** The closed. */
    private boolean closed = false;

    /** The session. */
    private EntityManagerSession session;

    /** The client map. */
    private Map<String, Client> clientMap;

    /** The event dispatcher. */
    private EntityEventDispatcher eventDispatcher;

    /** The is relation via join table. */
    boolean isRelationViaJoinTable;

    /** The no session lookup. */
    private boolean noSessionLookup;

    private FlushModeType flushMode = FlushModeType.AUTO;

    private ObjectGraphBuilder graphBuilder;

    private FlushManager flushManager;

    /**
     * Instantiates a new persistence delegator.
     * 
     * @param session
     *            the session
     * @param persistenceUnits
     *            the persistence units
     */
    public PersistenceDelegator(EntityManagerSession session)
    {
        this.session = session;
        eventDispatcher = new EntityEventDispatcher();
        graphBuilder = new ObjectGraphBuilder();
        flushManager = new FlushManager();
    }

    // TODO : This method needs serious attention!
    /**
     * Gets the client.
     * 
     * @param m
     *            the m
     * @return the client
     */
    public Client getClient(EntityMetadata m)
    {
        Client client = null;

        // Persistence Unit used to retrieve client
        String persistenceUnit = m.getPersistenceUnit();

        // single persistence unit given and entity is annotated with '@'.
        // validate persistence unit given is same

        // If client has already been created, return it, or create it and put
        // it into client map
        if (clientMap == null || clientMap.isEmpty())
        {
            clientMap = new HashMap<String, Client>();
            client = ClientResolver.discoverClient(persistenceUnit);
            clientMap.put(persistenceUnit, client);

        }
        else if (clientMap.get(persistenceUnit) == null)
        {
            client = ClientResolver.discoverClient(persistenceUnit);
            clientMap.put(persistenceUnit, client);
        }
        else
        {
            client = clientMap.get(persistenceUnit);
        }

        return client;
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
     * Gets the event dispatcher.
     * 
     * @return the event dispatcher
     */
    private EntityEventDispatcher getEventDispatcher()
    {
        return eventDispatcher;
    }

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param primaryKeys
     *            the primary keys
     * @return the list
     */
    public <E> List<E> find(Class<E> entityClass, Object... primaryKeys)
    {
        List<E> entities = new ArrayList<E>();
        Set pKeys = new HashSet(Arrays.asList(primaryKeys));
        for (Object primaryKey : pKeys)
        {
            entities.add(find(entityClass, primaryKey));
        }
        return entities;
    }

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param excludeGraph
     *            the exclude graph
     * @param primaryKeys
     *            the primary keys
     * @return the list
     */
    public <E> List<E> find(Class<E> entityClass, EntitySaveGraph excludeGraph, Object... primaryKeys)
    {
        List<E> entities = new ArrayList<E>();
        Set pKeys = new HashSet(Arrays.asList(primaryKeys));
        for (Object primaryKey : pKeys)
        {
            entities.add(find(entityClass, primaryKey, excludeGraph));
        }
        return entities;
    }

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param embeddedColumnMap
     *            the embedded column map
     * @return the list
     */
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        EntityMetadata entityMetadata = getMetadata(entityClass);

        List<E> entities = new ArrayList<E>();
        entities = getClient(entityMetadata).find(entityClass, embeddedColumnMap);

        return entities;
    }

    /**
     * Merge.
     * 
     * @param <E>
     *            the element type
     * @param e
     *            the e
     * @return the e
     */
    /*public <E> E merge2(E e)
    {

        List<EnhancedEntity> reachableEntities = EntityResolver.resolve(e, CascadeType.MERGE);

        // save each one
        // for (EnhancedEntity o : reachableEntities)
        // {
        log.debug("Merging Entity : " + e);

        EntityMetadata m = getMetadata(e.getClass());

        // TODO: throw OptisticLockException if wrong version and
        // optimistic locking enabled

        // fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PreUpdate.class);

        // Currently session look-up is not required for merge operation.
        // Once session implementation is mature this will not be required.
        noSessionLookup = true;
        persist(e);

        // fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PostUpdate.class);

        // reset session lookup.
        noSessionLookup = false;
        // }

        return e;
    }*/

    /**
     * Removes the.
     * 
     * @param e
     *            the e
     */
    /*@Deprecated
    public void remove2(Object e)
    {

        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PreRemove.class);

        EntityInterceptor interceptor = new EntityInterceptor();
        List<EntitySaveGraph> objectGraphs = interceptor.handleRelation(e, metadata);
        for (EntitySaveGraph objectGraph : objectGraphs)
        {
            removeGraph(objectGraph);
            // If cascade delete then delete parent and child. Else delete
            // marked entity only.
            // If parent entity is marked for delete
        }
        getEventDispatcher().fireEventListeners(metadata, e, PostPersist.class);
        log.debug("Data removed successfully for entity : " + e.getClass());

    }*/

    /**
     * Save graph.
     * 
     * @param objectGraph
     *            the object graph
     * @throws Exception
     *             the exception
     */
    private void removeGraph(EntitySaveGraph objectGraph)
    {
        EntityMetadata metadata = getMetadata(objectGraph.getParentClass());
        Object parentEntity = objectGraph.getParentEntity();

        // If this is a swapped graph and parent has further relations, delete
        // them before the parent
        List<EntitySaveGraph> relationGraphs = null;
        if (objectGraph.isIsswapped() && !metadata.getRelations().isEmpty() && parentEntity != null
                && !objectGraph.getChildClass().equals(objectGraph.getParentClass()))
        {
            relationGraphs = getGraph(parentEntity, metadata);
            List<EntitySaveGraph> uniqueGraphs = getDisjointGraph(objectGraph, relationGraphs);
            for (EntitySaveGraph g : uniqueGraphs)
            {
                removeGraph(g);
            }
        }

        // Delete parent entity
        if (parentEntity != null)
        {
            objectGraph.setParentId(getId(parentEntity, metadata));

            Client pClient = getClient(metadata);
            pClient.delete(parentEntity, objectGraph.getParentId());

            session.remove(parentEntity.getClass(), objectGraph.getParentEntity());
        }

        // Delete child entity
        Object childEntity = objectGraph.getChildEntity();
        // If any association exists.
        if (childEntity != null)
        {
            onClientHandle(objectGraph, childEntity);
        }

        // Delete data from Join Table
        deleteFromJoinTable(objectGraph, metadata);

    }

    /**
     * Delete from join table.
     * 
     * @param objectGraph
     *            the object graph
     * @param metadata
     *            the metadata
     */
    private void deleteFromJoinTable(EntitySaveGraph objectGraph, EntityMetadata metadata)
    {
        // Delete data from Join Table if any
        if (metadata.isRelationViaJoinTable())
        {
            for (Relation relation : metadata.getRelations())
            {
                if (relation.isRelatedViaJoinTable())
                {

                    JoinTableMetadata jtMetadata = relation.getJoinTableMetadata();
                    String joinTableName = jtMetadata.getJoinTableName();

                    Set<String> joinColumns = jtMetadata.getJoinColumns();
                    Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

                    String joinColumnName = (String) joinColumns.toArray()[0];
                    String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

                    EntityMetadata relMetadata = getMetadata(objectGraph.getChildClass());

                    Client pClient = getClient(metadata);
                    pClient.deleteByColumn(joinTableName, joinColumnName, objectGraph.getParentId());

                }
            }
        }
    }

    /**
     * On client persist.
     * 
     * @param objectGraph
     *            the object graph
     * @param childEntity
     *            the child entity
     * @throws Exception
     *             the exception
     */
    private void onClientHandle(EntitySaveGraph objectGraph, Object childEntity)
    {
        if (childEntity instanceof Collection<?>)
        {
            Collection<?> childCol = (Collection<?>) childEntity;
            for (Object ch : childCol)
            {
                if (ch != null)
                {
                    onClientDelete(ch, objectGraph);
                }
            }
        }
        else
        {
            if (childEntity != null)
            {
                onClientDelete(childEntity, objectGraph);
            }

        }
    }

    /**
     * Handle client.
     * 
     * @param child
     *            the child
     * @param objectGraph
     *            the object graph
     * @throws Exception
     *             the exception
     */
    private void onClientDelete(Object child, EntitySaveGraph objectGraph)
    {
        // If child entity doesn't have any further relations, just delete it
        // from database
        // Otherwise treat it as parent entity for its related entities,
        // determine graph and delete that graph recursively.

        EntityMetadata metadata = getMetadata(objectGraph.getChildClass());
        List<Relation> relations = metadata.getRelations();

        boolean imChildProcessed = false;

        List<EntitySaveGraph> objectGraphs = getGraph(child, metadata);
        if (!((relations == null || relations.isEmpty()) || objectGraph.isIsswapped()))
        {
            for (EntitySaveGraph graph : objectGraphs)
            {
                // This this graph is for an entity that has it's own
                // parent,
                // set reverse Foreign Key
                // i.e. Foreign key that refers to its parent
                if (!graph.equals(objectGraph))
                {
                    graph.setRevFKeyName(objectGraph.getfKeyName());
                    graph.setRevFKeyValue(objectGraph.getParentId());
                    graph.setRevParentClass(objectGraph.getParentClass());
                    imChildProcessed = true;
                    removeGraph(graph);
                }
            }
        }

        // In case immediate child is not yet processed!
        if (!imChildProcessed)
        {
            String id = getId(child, metadata);
            objectGraph.setChildId(id);
            // if (getSession().lookup(child.getClass(), id) == null)
            // {
            Client chClient = getClient(metadata);
            chClient.delete(child, id);
            // session.store(id, child);
            // }
        }

    }

    /**
     * Persist.
     * 
     * @param e
     *            the e
     */
    // Old implementation, to be deleted
    /*@Deprecated
    public void persist2(Object e)
    {
        // Invoke Pre Persist Events
        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PrePersist.class);

        // Get Object graph list for this top level entity, and save them
        // one by one.
        List<EntitySaveGraph> objectGraphs = getGraph(e, metadata);
        for (EntitySaveGraph objectGraph : objectGraphs)
        {
            saveGraph(objectGraph);
        }

        // Invoke Post Persist Events
        getEventDispatcher().fireEventListeners(metadata, e, PostPersist.class);
        log.debug("Data persisted successfully for entity : " + e.getClass());

    }*/

    

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param primaryKey
     *            the primary key
     * @param excludeGraph
     *            the exclude graph
     * @return the e
     */
    public <E> E find(Class<E> entityClass, Object primaryKey, EntitySaveGraph excludeGraph)
    {
        // Look up in session first
        E e = null/* getSession().lookup(entityClass, primaryKey) */;
        isRelationViaJoinTable = false;

        // if (null != e)
        // {
        // log.debug(entityClass.getName() + "_" + primaryKey +
        // " is loaded from cache!");
        // return e;
        // }

        // Find top level entity first
        EntityMetadata entityMetadata = getMetadata(entityClass);

        Client client = getClient(entityMetadata);

        Object object;
        try
        {
            object = entityMetadata.getEntityClazz().newInstance();
        }
        catch (InstantiationException e1)
        {
            throw new KunderaException("Error while finding object", e1);
        }
        catch (IllegalAccessException e1)
        {
            throw new KunderaException("Error while finding object", e1);
        }

        List<EntitySaveGraph> objectGraphs = getGraph(object, entityMetadata);
        List<EntitySaveGraph> graphs = getDisjointGraph(excludeGraph, objectGraphs);

        Map<Boolean, List<String>> relations = getRelations(graphs, entityMetadata.getEntityClazz());

        EntityReader reader = getReader(client);
        List<String> relationNames = relations.values().iterator().next();

        String rowKey = primaryKey + "";

        EnhanceEntity enhanceEntity = reader.findById(rowKey, entityMetadata, relationNames, client);

        Map<Object, Object> relationalValues = new HashMap<Object, Object>();
        if (enhanceEntity == null || enhanceEntity.getEntity() == null)
        {
            return null;
        }
        E entity = (E) enhanceEntity.getEntity();
        if (relationNames.isEmpty() && !entityMetadata.isRelationViaJoinTable())
        {
            return entity;
        }
        else
        {
            entity = (E) reader.computeGraph(enhanceEntity, graphs, relationalValues, client, entityMetadata, this);
        }
        boolean isCacheableToL2 = entityMetadata.isCacheable();
        getSession().store(primaryKey, entity, isCacheableToL2);

        // Populate Association,
        return entity;

    }

    /**
     * Gets the disjoint graph.
     * 
     * @param excludeGraph
     *            the exclude graph
     * @param objectGraphs
     *            the object graphs
     * @return the disjoint graph
     */
    private List<EntitySaveGraph> getDisjointGraph(EntitySaveGraph excludeGraph, List<EntitySaveGraph> objectGraphs)
    {
        List<EntitySaveGraph> graphs = new ArrayList<EntitySaveGraph>();

        for (EntitySaveGraph g : objectGraphs)
        {
            if (!((excludeGraph.getParentClass().equals(g.getParentClass()) || excludeGraph.getChildClass().equals(
                    g.getParentClass())) && (excludeGraph.getParentClass().equals(g.getChildClass()) || excludeGraph
                    .getChildClass().equals(g.getChildClass()))))
            {
                graphs.add(g);
            }
        }
        return graphs;
    }

    

    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param primaryKey
     *            the primary key
     * @return the e
     */
    /*@Deprecated    
    public <E> E find2(Class<E> entityClass, Object primaryKey)
    {

        isRelationViaJoinTable = false;

        // Find top level entity first
        EntityMetadata entityMetadata = getMetadata(entityClass);

        Client client = getClient(entityMetadata);

        Object object;
        try
        {
            object = entityMetadata.getEntityClazz().newInstance();
        }
        catch (InstantiationException e1)
        {
            throw new KunderaException("Error while finding object", e1);
        }
        catch (IllegalAccessException e1)
        {
            throw new KunderaException("Error while finding object", e1);
        }
        List<EntitySaveGraph> objectGraphs = getGraph(object, entityMetadata);
        Map<Boolean, List<String>> relations = getRelations(objectGraphs, entityMetadata.getEntityClazz());

        EntityReader reader = getReader(client);
        List<String> relationNames = relations.values().iterator().next();

        // String rowKey = primaryKey + "";

        EnhanceEntity enhanceEntity = reader.findById(primaryKey, entityMetadata, relationNames, client);

        Map<Object, Object> relationalValues = new HashMap<Object, Object>();
        if (enhanceEntity == null || enhanceEntity.getEntity() == null)
        {
            return null;
        }
        E entity = (E) enhanceEntity.getEntity();
        if (relationNames.isEmpty() && !entityMetadata.isRelationViaJoinTable())
        {
            return entity;
        }
        else
        {
            entity = (E) reader.computeGraph(enhanceEntity, objectGraphs, relationalValues, client, entityMetadata,
                    this);
        }
        boolean isCacheableToL2 = entityMetadata.isCacheable();
        getSession().store(primaryKey, entity, isCacheableToL2);

        // Populate Association,
        return entity;

    }
*/
    /**
     * Creates the query.
     * 
     * @param jpaQuery
     *            the jpa query
     * @return the query
     */
    public Query createQuery(String jpaQuery)
    {
        Query query = new QueryResolver().getQueryImplementation(jpaQuery, this);
        return query;
    }

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    public final boolean isOpen()
    {
        return !closed;
    }

    /**
     * Close.
     */
    public final void close()
    {
        eventDispatcher = null;

        // Close all clients created in this session
        if (clientMap != null && !clientMap.isEmpty())
        {
            for (Client client : clientMap.values())
            {
                client.close();
            }
            clientMap.clear();
            clientMap = null;
        }
        
        //TODO: Move all nodes tied to this EM into detached state
        clear();
        
        closed = true;
    }
    
    public final void clear() {
        //Move all nodes tied to this EM into detached state
        PersistenceCacheManager.clearPersistenceCache();
    }

    /**
     * Gets the metadata.
     * 
     * @param clazz
     *            the clazz
     * @return the metadata
     */
    public EntityMetadata getMetadata(Class<?> clazz)
    {
        return KunderaMetadataManager.getEntityMetadata(clazz);
    }

    /**
     * Saves an object graph to persistence stores. An object graph contains a
     * parent entity and one or more child entities at a time. There are other
     * attributes too that represent their relationship. *
     * 
     * @param objectGraph
     *            the object graph
     */
    private void saveGraph(EntitySaveGraph objectGraph)
    {
        Object parentEntity = objectGraph.getParentEntity();
        EntityMetadata metadata = getMetadata(objectGraph.getParentClass());

        List<EntitySaveGraph> relationGraphs = null;

        // If this is a swapped graph and parent has further relations, persist
        // before the parent
        if (objectGraph.isIsswapped() && !metadata.getRelations().isEmpty()
                && !objectGraph.getChildClass().equals(objectGraph.getParentClass()) && parentEntity != null)
        {

            relationGraphs = getGraph(parentEntity, metadata);

            for (EntitySaveGraph g : relationGraphs)
            {
                if (!(objectGraph.getChildClass().equals(g.getParentClass()) || objectGraph.getChildClass().equals(
                        g.getChildClass())))
                {
                    saveGraph(g);
                }

            }

        }

        // Persist parent entity
        if (parentEntity != null)
        {
            objectGraph.setParentId(getId(parentEntity, metadata));

            if (noSessionLookup
                    || (getSession().lookup(objectGraph.getParentClass(), objectGraph.getParentId()) == null))
            {
                Client pClient = getClient(metadata);
                pClient.persist(objectGraph, metadata);
                session.store(objectGraph.getParentId(), objectGraph.getParentEntity());
            }
        }

        // Persist child entity(ies)
        Object childEntity = objectGraph.getChildEntity();
        if (objectGraph.getParentEntity() != null && childEntity != null)
        {
            persistChildEntity(objectGraph, childEntity);

        }

        // Persist Join Table

        for (Relation relation : metadata.getRelations())
        {
            if (relation.isRelatedViaJoinTable())
            {

                JoinTableMetadata jtMetadata = relation.getJoinTableMetadata();
                String joinTableName = jtMetadata.getJoinTableName();

                Set<String> joinColumns = jtMetadata.getJoinColumns();
                Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

                String joinColumnName = (String) joinColumns.toArray()[0];
                String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

                EntityMetadata relMetadata = getMetadata(relation.getTargetEntity());

                Object child = objectGraph.getChildEntity();

                Client pClient = getClient(metadata);
                pClient.persistJoinTable(joinTableName, joinColumnName, inverseJoinColumnName, relMetadata,
                        objectGraph.getParentId(), child);

            }
        }

    }

    /**
     * Persist child entities of a given object graph into persistence store.
     * 
     * @param objectGraph
     *            the object graph
     * @param childEntity
     *            the child entity, It maybe one entity or a collection of
     *            entities.
     */
    private void persistChildEntity(EntitySaveGraph objectGraph, Object childEntity)
    {
        if (childEntity instanceof Collection<?>)
        {
            Collection<?> childCol = (Collection<?>) childEntity;
            for (Object ch : childCol)
            {
                persistOneChildEntity(ch, objectGraph);
            }

        }
        else
        {
            persistOneChildEntity(childEntity, objectGraph);
        }
    }

    /**
     * Persist one child entity into persistence store. Also checks whether this
     * child entity has further relationships. If yes, it generates a graph for
     * them and saves them recursively up to "n" level.
     * 
     * @param child
     *            the child Entity Object.
     * @param objectGraph
     *            the object graph to which this child entity belongs.
     */
    private void persistOneChildEntity(Object child, EntitySaveGraph objectGraph)
    {
        if (noSessionLookup
                || (objectGraph.getChildId() != null || getSession().lookup(child.getClass(), objectGraph.getChildId()) == null))
        {
            EntityMetadata metadata = getMetadata(objectGraph.getChildClass());

            boolean imChildProcessed = false;

            List<Relation> relations = metadata.getRelations();

            // If child entity doesn't have any further relations, just persist
            // it
            // into database
            // Otherwise treat it as parent entity for its related entities,
            // determine graph and save that graph recursively.

            // List<EntitySaveGraph> objectGraphs =
            // getDisjointGraph(objectGraph, getGraph(child, metadata));
            List<EntitySaveGraph> objectGraphs = getGraph(child, metadata);
            if (!((relations == null || relations.isEmpty()) || objectGraph.isIsswapped()))
            {

                for (EntitySaveGraph graph : objectGraphs)
                {
                    // This this graph is for an entity that has it's own
                    // parent,
                    // set reverse Foreign Key
                    // i.e. Foreign key that refers to its parent
                    if (!graph.equals(objectGraph))
                    {
                        graph.setRevFKeyName(objectGraph.getfKeyName());
                        graph.setRevFKeyValue(objectGraph.getParentId());
                        graph.setRevParentClass(objectGraph.getParentClass());
                        imChildProcessed = true;
                        saveGraph(graph);
                    }
                }
            }

            // In case immediate child is not yet processed!
            if (!imChildProcessed)
            {
                saveImmediateChild(child, objectGraph, metadata);
            }

        }
    }

    /**
     * Save immediate child.
     * 
     * @param child
     *            the child
     * @param objectGraph
     *            the object graph
     * @param metadata
     *            the metadata
     */
    private void saveImmediateChild(Object child, EntitySaveGraph objectGraph, EntityMetadata metadata)
    {
        String id = getId(child, metadata);
        if (noSessionLookup || (getSession().lookup(child.getClass(), id) == null))
        {
            objectGraph.setChildId(id);
            // if (getSession().lookup(child.getClass(), id) == null)
            // {
            Client chClient = getClient(metadata);
            chClient.persist(child, objectGraph, metadata);
            session.store(id, child);
        }
    }

    /**
     * Gets the id.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * @return the id
     */
    public String getId(Object entity, EntityMetadata metadata)
    {
        try
        {
            return PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e)
        {
            throw new KunderaException(e);
        }

    }

    /**
     * Returns entity save graph collection for given entity.
     * 
     * @param entity
     *            entity in question
     * @param metadata
     *            entity's metadata
     * @return Collection of entity save graph
     */
    public List<EntitySaveGraph> getGraph(Object entity, EntityMetadata metadata)
    {
        EntityInterceptor interceptor = new EntityInterceptor();
        return interceptor.handleRelation(entity, metadata);
    }

    /**
     * Store.
     * 
     * @param id
     *            the id
     * @param entity
     *            the entity
     */
    public void store(Object id, Object entity)
    {
        session.store(id, entity);
    }

    /**
     * Store.
     * 
     * @param entities
     *            the entities
     * @param entityMetadata
     *            the entity metadata
     */
    public void store(List entities, EntityMetadata entityMetadata)
    {
        for (Object o : entities)
            session.store(getId(o, entityMetadata), o);
    }

    /**
     * Gets the relations.
     * 
     * @param graphs
     *            the graphs
     * @param clazz
     *            the clazz
     * @return the relations
     */
    public Map<Boolean, List<String>> getRelations(List<EntitySaveGraph> graphs, Class clazz)
    {
        List<String> relationNames = new ArrayList<String>(graphs.size());
        boolean isParent = false;
        Map<Boolean, List<String>> relationHolder = new HashMap<Boolean, List<String>>(1);
        // TODO need to check if there is any relation?
        for (EntitySaveGraph g : graphs)
        {
            isParent = false;
            if (clazz.equals(g.getParentClass()))
            {
                isParent = true;
                // Means entity is parent
            }

            if (g.getfKeyName() != null)
            {
                relationNames.add(g.getfKeyName());
            }
        }

        relationHolder.put(isParent, relationNames);
        return relationHolder;

    }

    /**
     * Gets the reader.
     * 
     * @param client
     *            the client
     * @return the reader
     */
    public EntityReader getReader(Client client)
    {
        return client.getReader();
    }

    

    /**
     * @return the flushMode
     */
    public FlushModeType getFlushMode()
    {
        return flushMode;
    }

    /**
     * @param flushMode
     *            the flushMode to set
     */
    public void setFlushMode(FlushModeType flushMode)
    {
        this.flushMode = flushMode;
    }

    // /////////////////////////////New and Modified Implementations////////////////////////////////////
    // ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Writes an entity into Persistence cache
     */
    
    public void persist(Object e)
    {
        // Invoke Pre Persist Events
        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PrePersist.class);

        // Create an object graph of the entity object
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new TransientState());

        // Call persist on each node in object graph
        Node headNode = graph.getHeadNode();
        if(headNode.getParents() == null) {
            headNode.setHeadNode(true);
        }
        
        headNode.persist();

        // TODO: not always, should be conditional
        flush();

        // Invoke Post Persist Events
        getEventDispatcher().fireEventListeners(metadata, e, PostPersist.class);
        log.debug("Data persisted successfully for entity : " + e.getClass());
    }
    
    /**
     * Finds an entity from persistence cache, if not there, fetches from database
     * @param <E>
     * @param entityClass
     * @param primaryKey
     * @return
     */
    public <E> E find(Class<E> entityClass, Object primaryKey)
    {

        EntityMetadata entityMetadata = getMetadata(entityClass);

        String nodeId = ObjectGraphBuilder.getNodeId(primaryKey, entityClass);

        MainCache mainCache = (MainCache)PersistenceCache.INSTANCE.getMainCache();
        Node node = mainCache.getNodeFromCache(nodeId);

        // if node is not in persistence cache or is dirty, fetch from database
        if (node == null || node.isDirty())
        {

            node = new Node(nodeId, entityClass, new ManagedState());
            Client client = getClient(entityMetadata);
            node.setClient(client);
            node.setPersistenceDelegator(this);

            node.find();
        }
        
        Object nodeData = node.getData();  
        
        //If node for this nodeData is not already there in PC, 
        //Generate an object graph of this found entity, and put it into cache with Managed state
        if(nodeData != null) {
            if(PersistenceCache.INSTANCE.getMainCache().getNodeFromCache(nodeId) == null) {
                ObjectGraph graph = new ObjectGraphBuilder().getObjectGraph(nodeData, new ManagedState());
                PersistenceCache.INSTANCE.getMainCache().addGraphToCache(graph);
            }
            
        }     
        
        
        
        return (E) nodeData;
    }
    
   
    /**
     * Removes an entity object from persistence cache 
     */
    public void remove(Object e)
    {

        // Invoke Pre Remove Events
        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PreRemove.class);

        // Create an object graph of the entity object
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new ManagedState());

        Node headNode = graph.getHeadNode();
        
        if(headNode.getParents() == null) {
            headNode.setHeadNode(true);
        }        

        headNode.remove();

        // TODO: not always, should be conditional
        flush();

        getEventDispatcher().fireEventListeners(metadata, e, PostRemove.class);
        log.debug("Data removed successfully for entity : " + e.getClass());

    }
    
    
    
    /**
     * Flushes Dirty objects in {@link PersistenceCache} to databases.
     */
    public void flush()
    {
        // Check for flush mode, if commit, do nothing (state will be updated at
        // commit) else if AUTO, synchronize with DB
        if (FlushModeType.COMMIT.equals(getFlushMode()))
        {
            // Do nothing
        }
        else if (FlushModeType.AUTO.equals(getFlushMode()))
        {
            // Build Flush Stack from the Persistence Cache
            // TODO: Cascade flush for only those related entities for whom
            // cascade=ALL or PERSIST
            flushManager.buildFlushStack(PersistenceCache.INSTANCE);

            // Get flush stack from Persistence Cache
            FlushStack fs = PersistenceCache.INSTANCE.getFlushStack();

            // Flush each node in flush stack from top to bottom unit it's empty
            log.debug("Flushing following flush stack to database(s) (showing stack objects from top to bottom):\n"
                    + fs);
            while (!fs.isEmpty())
            {
                Node node = fs.pop();
                
                //Only nodes in Managed and Removed state are flushed, rest are ignored
                if (node.isInState(ManagedState.class) || node.isInState(RemovedState.class))
                {
                    EntityMetadata metadata = getMetadata(node.getDataClass());
                    node.setClient(getClient(metadata));

                    node.flush();

                    // Update Link value for all nodes attached to this one
                    Map<NodeLink, Node> parents = node.getParents();
                    Map<NodeLink, Node> children = node.getChildren();

                    if (parents != null && !parents.isEmpty())
                    {
                        for (NodeLink parentNodeLink : parents.keySet())
                        {
                            parentNodeLink.addLinkProperty(LinkProperty.LINK_VALUE,
                                    ObjectGraphBuilder.getEntityId(node.getNodeId()));
                        }
                    }

                    if (children != null && !children.isEmpty())
                    {
                        for (NodeLink childNodeLink : children.keySet())
                        {
                            childNodeLink.addLinkProperty(LinkProperty.LINK_VALUE,
                                    ObjectGraphBuilder.getEntityId(node.getNodeId()));
                        }
                    }
                }

            }
            
            //Flush Join Table data into database
            Map<String, JoinTableData> joinTableDataMap = PersistenceCache.INSTANCE.getJoinTableDataMap();
            for(JoinTableData jtData : joinTableDataMap.values()) {
                EntityMetadata m = KunderaMetadataManager.getEntityMetadata(jtData.getEntityClass());
                Client client = getClient(m);
                
                if(OPERATION.INSERT.equals(jtData.getOperation())) {
                    client.persistJoinTable(jtData);
                } else if(OPERATION.DELETE.equals(jtData.getOperation())) {
                    for(Object pk : jtData.getJoinTableRecords().keySet()) {
                        client.deleteByColumn(jtData.getJoinTableName(), m.getIdColumn().getName(), pk);
                    }                   
                }                
            }
            joinTableDataMap.clear();   //All Join table operation performed, clear it.           
        }
    }
    
    public <E> E merge(E e)
    {     

        log.debug("Merging Entity : " + e);
        EntityMetadata m = getMetadata(e.getClass());

        // TODO: throw OptisticLockException if wrong version and
        // optimistic locking enabled

        //Fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PreUpdate.class);
        
        // Create an object graph of the entity object to be merged
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new ManagedState());

        //Call merge on each node in object graph
        Node headNode = graph.getHeadNode();
        if(headNode.getParents() == null) {
            headNode.setHeadNode(true);
        }        
        headNode.merge();

        // TODO: not always, should be conditional
        flush();

        // fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PostUpdate.class);    

        return e;
    }

}
