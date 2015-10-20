/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import javax.persistence.CascadeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.lifecycle.states.TransientState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.EventLog.EventType;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

/**
 * Provides utility methods for managing Flush Stack.
 * 
 * @author amresh.singh
 */
public class FlushManager
{

    /**
     * Deque containing Nodes to be flushed Entities are always flushed from the
     * start, there way to end until deque is empty.
     */
    private Deque<Node> stackQueue;

    /**
     * Map containing data required for inserting records for each join table.
     * Key -> Name of Join Table Value -> records to be persisted in the join
     * table
     */
    private List<JoinTableData> joinTableDataCollection = new ArrayList<JoinTableData>();

    /** The event log queue. */
    private EventLogQueue eventLogQueue = new EventLogQueue();

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(FlushManager.class);

    private static Map<EventType, List<CascadeType>> cascadePermission = new HashMap<EventType, List<CascadeType>>();

    static
    {
        List<CascadeType> cascades = new ArrayList<CascadeType>();
        cascades.add(CascadeType.ALL);
        cascades.add(CascadeType.PERSIST);

        cascadePermission.put(EventType.INSERT, cascades);
        
        cascades = new ArrayList<CascadeType>();
        cascades.add(CascadeType.ALL);
        cascades.add(CascadeType.MERGE);
        cascadePermission.put(EventType.UPDATE, cascades);
        
        cascades = new ArrayList<CascadeType>();
        cascades.add(CascadeType.ALL);
        cascades.add(CascadeType.REMOVE);
        cascadePermission.put(EventType.DELETE, cascades);

        // cascades.remove(CascadeType.REMOVE);
        // cascades.add(CascadeType.REFRESH);
        // cascadePermission.put(com.impetus.kundera.lifecycle.states.NodeState.OPERATION.REFRESH,
        // cascades);

        // cascades.remove(CascadeType.REFRESH);
        // cascades.add(CascadeType.DETACH);
        // cascadePermission.put(com.impetus.kundera.lifecycle.states.NodeState.OPERATION.DETACH,
        // cascades);
    }

    /**
     * Instantiates a new flush manager.
     */
    public FlushManager()
    {
        stackQueue = new LinkedBlockingDeque<Node>();
    }

    /**
     * Builds the flush stack.
     * 
     * @param headNode
     *            the head node
     * @param eventType
     *            the event type
     */
    public void buildFlushStack(Node headNode, EventType eventType)
    {
        if (headNode != null)
        {
            headNode.setTraversed(false);
            addNodesToFlushStack(headNode, eventType);
        }
    }

    /**
     * Adds the nodes to flush stack.
     * 
     * @param node
     *            the node
     * @param eventType
     *            the event type
     */
    private void addNodesToFlushStack(Node node, EventType eventType)
    {

        Map<NodeLink, Node> children = node.getChildren();

        performOperation(node, eventType);

        // If this is a leaf node (not having any child, no need to go any
        // deeper
        if (children != null)
        {
            Map<NodeLink, Node> oneToOneChildren = new HashMap<NodeLink, Node>();
            Map<NodeLink, Node> oneToManyChildren = new HashMap<NodeLink, Node>();
            Map<NodeLink, Node> manyToOneChildren = new HashMap<NodeLink, Node>();
            Map<NodeLink, Node> manyToManyChildren = new HashMap<NodeLink, Node>();

            for (NodeLink nodeLink : children.keySet())
            {
                List<CascadeType> cascadeTypes = (List<CascadeType>) nodeLink.getLinkProperty(LinkProperty.CASCADE);

                if (cascadeTypes.contains(cascadePermission.get(eventType).get(0))
                        || cascadeTypes.contains(cascadePermission.get(eventType).get(1)))
                {
                    Relation.ForeignKey multiplicity = nodeLink.getMultiplicity();

                    Node childNode = children.get(nodeLink);

                    switch (multiplicity)
                    {
                    case ONE_TO_ONE:
                        oneToOneChildren.put(nodeLink, childNode);
                        break;
                    case ONE_TO_MANY:
                        oneToManyChildren.put(nodeLink, childNode);
                        break;
                    case MANY_TO_ONE:
                        manyToOneChildren.put(nodeLink, childNode);
                        break;
                    case MANY_TO_MANY:
                        manyToManyChildren.put(nodeLink, childNode);
                        break;
                    }
                }
            }

            // Process One-To-Many children
            for (NodeLink nodeLink : oneToManyChildren.keySet())
            {
                // Process child node Graph recursively first
                Node childNode = children.get(nodeLink);

                if (childNode != null && !childNode.isTraversed())
                {
                    addNodesToFlushStack(childNode, eventType);
                }
            }

            // Process Many-To-Many children
            for (NodeLink nodeLink : manyToManyChildren.keySet())
            {
                if (!node.isTraversed() && !(Boolean) nodeLink.getLinkProperty(LinkProperty.IS_RELATED_VIA_JOIN_TABLE))
                {
                    // Push this node to stack
                    node.setTraversed(true);
                    stackQueue.push(node);
                    logEvent(node, eventType);
                }

                Node childNode = children.get(nodeLink);

                if (childNode != null)
                {
                    // Extract information required to be persisted into
                    // Join
                    // Table
                    if (node.isDirty() && !node.isTraversed())
                    {
                        // M-2-M relation fields that are Set or List are joined
                        // by join table.
                        // M-2-M relation fields that are Map aren't joined by
                        // Join table

                        JoinTableMetadata jtmd = (JoinTableMetadata) nodeLink
                                .getLinkProperty(LinkProperty.JOIN_TABLE_METADATA);
                        if (jtmd != null)
                        {
                            String joinColumnName = (String) jtmd.getJoinColumns().toArray()[0];
                            String inverseJoinColumnName = (String) jtmd.getInverseJoinColumns().toArray()[0];
                            Object entityId = node.getEntityId();
                            Object childId = childNode.getEntityId();
                            Set<Object> childValues = new HashSet<Object>();
                            childValues.add(childId);

                            OPERATION operation = null;
                            if (node.getCurrentNodeState().getClass().equals(ManagedState.class))
                            {
                                operation = OPERATION.INSERT;
                            }
                            else if (node.getCurrentNodeState().getClass().equals(RemovedState.class))
                            {
                                operation = OPERATION.DELETE;
                            }

                            addJoinTableData(operation, jtmd.getJoinTableSchema(), jtmd.getJoinTableName(),
                                    joinColumnName, inverseJoinColumnName, node.getDataClass(), entityId, childValues);
                        }
                    }

                    // Process child node Graph recursively first
                    if (!childNode.isTraversed())
                    {
                        addNodesToFlushStack(childNode, eventType);
                    }
                }
            }
            // Process One-To-One children
            for (NodeLink nodeLink : oneToOneChildren.keySet())
            {
                if (!node.isTraversed())
                {
                    // Push this node to stack
                    node.setTraversed(true);
                    stackQueue.push(node);
                    logEvent(node, eventType);
                }

                // Process child node Graph recursively
                Node childNode = children.get(nodeLink);

                addNodesToFlushStack(childNode, eventType);
            }

            // Process Many-To-One children
            for (NodeLink nodeLink : manyToOneChildren.keySet())
            {
                if (!node.isTraversed())
                {
                    // Push this node to stack
                    node.setTraversed(true);
                    stackQueue.push(node);
                    logEvent(node, eventType);
                }

                // Child node of this node
                Node childNode = children.get(nodeLink);

                // Process all parents of child node with Many-To-One
                // relationship first
                Map<NodeLink, Node> parents = childNode.getParents();
                for (NodeLink parentLink : parents.keySet())
                {
                    List<CascadeType> cascadeTypes = (List<CascadeType>) nodeLink.getLinkProperty(LinkProperty.CASCADE);

                    if (cascadeTypes.contains(cascadePermission.get(eventType).get(0))
                            || cascadeTypes.contains(cascadePermission.get(eventType).get(1)))
                    {
                        Relation.ForeignKey multiplicity = parentLink.getMultiplicity();

                        Node parentNode = parents.get(parentLink);

//                        performOperation(parentNode, eventType);

                        if (multiplicity.equals(Relation.ForeignKey.MANY_TO_ONE))
                        {
                            if (!parentNode.isTraversed() && parentNode.isDirty())
                            {
                                addNodesToFlushStack(parentNode, eventType);
                            }
                        }
                    }
                }

                // Finally process this child node
                if (!childNode.isTraversed() && childNode.isDirty())
                {
                    addNodesToFlushStack(childNode, eventType);
                }
                else if (!childNode.isDirty())
                {
                    childNode.setTraversed(true);
                    stackQueue.push(childNode);
                    logEvent(childNode, eventType);
                }
            }
        }

        // Finally, if this node itself is not traversed yet, (as may happen
        // in
        // 1-1 and M-1
        // cases), push it to stack
        if (!node.isTraversed() && node.isDirty())
        {
            node.setTraversed(true);
            stackQueue.push(node);
            logEvent(node, eventType);
        }
    }

    /*    *//**
     * Gets the flush stack.
     * 
     * @return the flushStack
     */
    public Deque<Node> getFlushStack()
    {
        return stackQueue;
    }

    /**
     * Gets the join table data map.
     * 
     * @return the joinTableDataMap
     */
    public List<JoinTableData> getJoinTableData()
    {
        return joinTableDataCollection;
    }

    /**
     * Empties Flush stack present in a PersistenceCache.
     * 
     */
    public void clearFlushStack()
    {
        if (stackQueue != null && !stackQueue.isEmpty())
        {
            stackQueue.clear();
        }
        if (joinTableDataCollection != null && !joinTableDataCollection.isEmpty())
        {
            joinTableDataCollection.clear();
        }

        if (eventLogQueue != null)
        {
            eventLogQueue.clear();
        }
    }

    /**
     * Rollback.
     * 
     * @param delegator
     *            the delegator
     */
    public void rollback(PersistenceDelegator delegator)
    {
        if (eventLogQueue != null)
        {
            onRollBack(delegator, eventLogQueue.getInsertEvents());
            onRollBack(delegator, eventLogQueue.getUpdateEvents());
            onRollBack(delegator, eventLogQueue.getDeleteEvents());

            rollbackJoinTableData(delegator);
        }
    }

    /**
     * Rollback.
     * 
     * @param delegator
     *            the delegator
     */
    public void commit()
    {
        onCommit(eventLogQueue.getInsertEvents());
        onCommit(eventLogQueue.getUpdateEvents());
        onCommit(eventLogQueue.getDeleteEvents());
    }

    /**
     * @param deleteEvents
     */
    private void onCommit(Map<Object, EventLog> eventCol)
    {
        if (eventCol != null && !eventCol.isEmpty())
        {
            Collection<EventLog> events = eventCol.values();
            Iterator<EventLog> iter = events.iterator();

            while (iter.hasNext())
            {
                try
                {
                    EventLog event = iter.next();
                    Node node = event.getNode();
                    if (node.isProcessed())
                    {
                        // One time set as required for rollback.
                        Node original = node.clone();
                        node.setOriginalNode(original);
                    }

                    // mark it null for garbage collection.
                    event = null;
                }
                catch (Exception ex)
                {
                    log.warn("Caught exception during rollback, Caused by:", ex);
                    // bypass to next event
                }

            }
        }
    }

    /**
     * On roll back.
     * 
     * @param delegator
     *            the delegator
     * @param eventCol
     *            the event col
     */
    private void onRollBack(PersistenceDelegator delegator, Map<Object, EventLog> eventCol)
    {
        if (eventCol != null && !eventCol.isEmpty())
        {
            Collection<EventLog> events = eventCol.values();
            Iterator<EventLog> iter = events.iterator();

            while (iter.hasNext())
            {
                try
                {
                    EventLog event = iter.next();
                    Node node = event.getNode();
                    Class clazz = node.getDataClass();
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(), clazz);
                    Client client = delegator.getClient(metadata);

                    // do manual rollback, if data is processed, and running
                    // without transaction or with kundera's default transaction
                    // support!
                    if (node.isProcessed()
                            && (!delegator.isTransactionInProgress() || MetadataUtils
                                    .defaultTransactionSupported(metadata.getPersistenceUnit(), delegator.getKunderaMetadata())))
                    {
                        if (node.getOriginalNode() == null)
                        {
                            Object entityId = node.getEntityId();
                            client.remove(node.getData(), entityId);
                        }
                        else
                        {
                            client.persist(node.getOriginalNode());
                        }
                    }
                    // mark it null for garbage collection.
                    event = null;
                }
                catch (Exception ex)
                {
                    log.warn("Caught exception during rollback, Caused by:", ex);
                    // bypass to next event
                }
            }
        }
        // mark it null for garbage collection.
        eventCol = null;
    }

    /**
     * Adds the join table data into map.
     * 
     * @param operation
     *            the operation
     * @param joinTableName
     *            the join table name
     * @param joinColumnName
     *            the join column name
     * @param invJoinColumnName
     *            the inv join column name
     * @param entityClass
     *            the entity class
     * @param joinColumnValue
     *            the join column value
     * @param invJoinColumnValues
     *            the inv join column values
     */
    private void addJoinTableData(OPERATION operation, String schemaName, String joinTableName, String joinColumnName,
            String invJoinColumnName, Class<?> entityClass, Object joinColumnValue, Set<Object> invJoinColumnValues)
    {
        JoinTableData joinTableData = new JoinTableData(operation, schemaName, joinTableName, joinColumnName,
                invJoinColumnName, entityClass);
        joinTableData.addJoinTableRecord(joinColumnValue, invJoinColumnValues);
        joinTableDataCollection.add(joinTableData);
    }

    /**
     * Log event.
     * 
     * @param node
     *            the node
     * @param eventType
     *            the event type
     */
    private void logEvent(Node node, EventType eventType)
    {
        // Node contains original as well as transactional copy.
        EventLog log = new EventLog(eventType, node);
        eventLogQueue.onEvent(log, eventType);
    }

    private void rollbackJoinTableData(PersistenceDelegator delegator)
    {
        // on deleting join table data.
        for (JoinTableData jtData : joinTableDataCollection)
        {
            if (jtData.isProcessed())
            {
                EntityMetadata m = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(), jtData.getEntityClass());
                Client client = delegator.getClient(m);

                if (OPERATION.INSERT.equals(jtData.getOperation()))
                {
                    for (Object pk : jtData.getJoinTableRecords().keySet())
                    {
                        client.deleteByColumn(jtData.getSchemaName(), jtData.getJoinTableName(), m.getIdAttribute()
                                .getName(), pk);
                    }
                }
                else if (OPERATION.DELETE.equals(jtData.getOperation()))
                {
                    client.persistJoinTable(jtData);
                }
            }
        }
        joinTableDataCollection.clear();
        joinTableDataCollection = null;
        joinTableDataCollection = new ArrayList<JoinTableData>();
    }

    /**
     * @param nodeStateContext
     */
    private void performOperation(Node node, EventType eventType)
    {
        switch (eventType)
        {
        case INSERT:
            node.persist();
            break;

        case UPDATE:
            if (node.isInState(TransientState.class))
            {
                node.persist();
            }
            else
            {
                node.merge();
            }
            break;

        case DELETE:
            node.remove();
            break;
        }
    }

}
