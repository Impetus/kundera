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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.impetus.kundera.persistence.context.FlushManager;
import com.impetus.kundera.persistence.context.FlushStack;
import com.impetus.kundera.persistence.context.MainCache;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;
import com.impetus.kundera.persistence.event.EntityEventDispatcher;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
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

    private FlushModeType flushMode = FlushModeType.AUTO;

    private ObjectGraphBuilder graphBuilder;

    private FlushManager flushManager;
    
    //Whether a transaction is in progress
    private boolean isTransactionInProgress;
    
    private PersistenceCache persistenceCache;

    /**
     * Instantiates a new persistence delegator.
     * 
     * @param session
     *            the session
     * @param persistenceUnits
     *            the persistence units
     */
    public PersistenceDelegator(EntityManagerSession session, PersistenceCache pc)
    {
        this.session = session;
        eventDispatcher = new EntityEventDispatcher();
        graphBuilder = new ObjectGraphBuilder();
        flushManager = new FlushManager();
        this.persistenceCache = pc;
    }
    
    
    /***********************************************************************/
    /***************** CRUD Methods ****************************************/
    /***********************************************************************/
    

    /**
     * Writes an entity into Persistence cache
     */

    public void persist(Object e)
    {
        // Invoke Pre Persist Events
        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PrePersist.class);

        // Create an object graph of the entity object
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new TransientState(), getPersistenceCache());

        // Call persist on each node in object graph
        Node headNode = graph.getHeadNode();
        if (headNode.getParents() == null)
        {
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
     * Finds an entity from persistence cache, if not there, fetches from
     * database
     * 
     * @param <E>
     * @param entityClass
     * @param primaryKey
     * @return
     */
    public <E> E find(Class<E> entityClass, Object primaryKey)
    {

        EntityMetadata entityMetadata = getMetadata(entityClass);

        String nodeId = ObjectGraphBuilder.getNodeId(primaryKey, entityClass);

        MainCache mainCache = (MainCache) getPersistenceCache().getMainCache();
        Node node = mainCache.getNodeFromCache(nodeId);

        // if node is not in persistence cache or is dirty, fetch from database
        if (node == null || node.isDirty())
        {

            node = new Node(nodeId, entityClass, new ManagedState(), getPersistenceCache());
            Client client = getClient(entityMetadata);
            node.setClient(client);
            node.setPersistenceDelegator(this);

            node.find();
        }

        Object nodeData = node.getData();

        // If node for this nodeData is not already there in PC,
        // Generate an object graph of this found entity, and put it into cache
        // with Managed state
        if (nodeData != null)
        {
            if (getPersistenceCache().getMainCache().getNodeFromCache(nodeId) == null)
            {
                ObjectGraph graph = new ObjectGraphBuilder().getObjectGraph(nodeData, new ManagedState(), getPersistenceCache());
                getPersistenceCache().getMainCache().addGraphToCache(graph, getPersistenceCache());
            }

        }

        return (E) nodeData;
    }
    
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
     * Removes an entity object from persistence cache
     */
    public void remove(Object e)
    {

        // Invoke Pre Remove Events
        EntityMetadata metadata = getMetadata(e.getClass());
        getEventDispatcher().fireEventListeners(metadata, e, PreRemove.class);

        // Create an object graph of the entity object
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new ManagedState(), getPersistenceCache());

        Node headNode = graph.getHeadNode();

        if (headNode.getParents() == null)
        {
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
            flushManager.buildFlushStack(getPersistenceCache());

            // Get flush stack from Persistence Cache
            FlushStack fs = getPersistenceCache().getFlushStack();

            // Flush each node in flush stack from top to bottom unit it's empty
            log.debug("Flushing following flush stack to database(s) (showing stack objects from top to bottom):\n"
                    + fs);
            while (!fs.isEmpty())
            {
                Node node = fs.pop();

                // Only nodes in Managed and Removed state are flushed, rest are
                // ignored
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

            // Flush Join Table data into database
            Map<String, JoinTableData> joinTableDataMap = getPersistenceCache().getJoinTableDataMap();
            for (JoinTableData jtData : joinTableDataMap.values())
            {
                EntityMetadata m = KunderaMetadataManager.getEntityMetadata(jtData.getEntityClass());
                Client client = getClient(m);

                if (OPERATION.INSERT.equals(jtData.getOperation()))
                {
                    client.persistJoinTable(jtData);
                }
                else if (OPERATION.DELETE.equals(jtData.getOperation()))
                {
                    for (Object pk : jtData.getJoinTableRecords().keySet())
                    {
                        client.deleteByColumn(jtData.getJoinTableName(), m.getIdColumn().getName(), pk);
                    }
                }
            }
            joinTableDataMap.clear(); // All Join table operation performed,
                                      // clear it.
        }
    }

    public <E> E merge(E e)
    {

        log.debug("Merging Entity : " + e);
        EntityMetadata m = getMetadata(e.getClass());

        // TODO: throw OptisticLockException if wrong version and
        // optimistic locking enabled

        // Fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PreUpdate.class);

        // Create an object graph of the entity object to be merged
        ObjectGraph graph = graphBuilder.getObjectGraph(e, new ManagedState(), getPersistenceCache());

        // Call merge on each node in object graph
        Node headNode = graph.getHeadNode();
        if (headNode.getParents() == null)
        {
            headNode.setHeadNode(true);
        }
        headNode.merge();

        // TODO: not always, should be conditional
        flush();

        // fire PreUpdate events
        getEventDispatcher().fireEventListeners(m, e, PostUpdate.class);

        return e;
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

        // TODO: Move all nodes tied to this EM into detached state
        clear();

        closed = true;
    }

    public final void clear()
    {
        // Move all nodes tied to this EM into detached state
        new PersistenceCacheManager(getPersistenceCache()).clearPersistenceCache();
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
    

    /**
     * @return the isTransactionInProgress
     */
    public boolean isTransactionInProgress()
    {
        return isTransactionInProgress;
    } 


    /**
     * @return the persistenceCache
     */
    public PersistenceCache getPersistenceCache()
    {
        return persistenceCache;
    }


    /******************************* Transaction related methods ***********************************************/

    
    
    public void begin()
    {
        isTransactionInProgress = true;
    }

    public void commit()
    {
        flush();
        isTransactionInProgress = false;
    }

    public void rollback()
    {
        isTransactionInProgress = false;
    }

    public boolean getRollbackOnly()
    {
        return false;
    }

    public void setRollbackOnly()
    {

    }

    public boolean isActive()
    {
        return isTransactionInProgress;
    }    
}
