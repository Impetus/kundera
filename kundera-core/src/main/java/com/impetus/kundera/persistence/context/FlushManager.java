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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.EventLog.EventType;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

// TODO: Auto-generated Javadoc
/**
 * Provides utility methods for managing Flush Stack.
 * 
 * @author amresh.singh
 */
public class FlushManager
{

    /**
     * Stack containing Nodes to be flushed Entities are always flushed from the
     * top, there way to bottom until stack is empty.
     */
    private FlushStack flushStack;

    /**
     * Map containing data required for inserting records for each join table.
     * Key -> Name of Join Table Value -> records to be persisted in the join
     * table
     */
    private Map<String, JoinTableData> joinTableDataMap;

    /** The event log queue. */
    private EventLogQueue eventLogQueue = new EventLogQueue();

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(FlushManager.class);

    /**
     * Instantiates a new flush manager.
     */
    public FlushManager()
    {
        flushStack = new FlushStack();
        joinTableDataMap = new HashMap<String, JoinTableData>();
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
        headNode.setTraversed(false);
        addNodesToFlushStack(headNode, eventType);
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
                Relation.ForeignKey multiplicity = nodeLink.getMultiplicity();

                switch (multiplicity)
                {
                case ONE_TO_ONE:
                    oneToOneChildren.put(nodeLink, children.get(nodeLink));
                    break;
                case ONE_TO_MANY:
                    oneToManyChildren.put(nodeLink, children.get(nodeLink));
                    break;
                case MANY_TO_ONE:
                    manyToOneChildren.put(nodeLink, children.get(nodeLink));
                    break;
                case MANY_TO_MANY:
                    manyToManyChildren.put(nodeLink, children.get(nodeLink));
                    break;
                }

            }

            // Process One-To-Many children
            for (NodeLink nodeLink : oneToManyChildren.keySet())
            {
                // Process child node Graph recursively first
                // Node childNode =
                // mainCache.getNodeFromCache(nodeLink.getTargetNodeId());
                Node childNode = children.get(nodeLink);

                if (!childNode.isTraversed())
                {
                    addNodesToFlushStack(childNode, eventType);
                }

            }

            // Process Many-To-Many children
            for (NodeLink nodeLink : manyToManyChildren.keySet())
            {
                // Node childNode =
                // mainCache.getNodeFromCache(nodeLink.getTargetNodeId());
                Node childNode = children.get(nodeLink);

                if (childNode != null)
                {
                    // Extract information required to be persisted into
                    // Join
                    // Table
                    if (node.isDirty() && !node.isTraversed())
                    {
                        JoinTableMetadata jtmd = (JoinTableMetadata) nodeLink
                                .getLinkProperty(LinkProperty.JOIN_TABLE_METADATA);
                        if (jtmd != null)
                        {
                            String joinColumnName = (String) jtmd.getJoinColumns().toArray()[0];
                            String inverseJoinColumnName = (String) jtmd.getInverseJoinColumns().toArray()[0];
                            Object entityId = ObjectGraphBuilder.getEntityId(node.getNodeId());
                            Object childId = ObjectGraphBuilder.getEntityId(childNode.getNodeId());

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

                            addJoinTableDataIntoMap(operation, jtmd.getJoinTableName(), joinColumnName,
                                    inverseJoinColumnName, node.getDataClass(), entityId, childValues);
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
                    flushStack.push(node);
                    logEvent(node, eventType);

                    // Process child node Graph recursively
                    // Node childNode =
                    // mainCache.getNodeFromCache(nodeLink.getTargetNodeId());
                    Node childNode = children.get(nodeLink);
                    addNodesToFlushStack(childNode, eventType);
                }
            }

            // Process Many-To-One children
            for (NodeLink nodeLink : manyToOneChildren.keySet())
            {
                if (!node.isTraversed())
                {
                    // Push this node to stack
                    node.setTraversed(true);
                    flushStack.push(node);
                    logEvent(node, eventType);
                }

                // Child node of this node
                // Node childNode =
                // mainCache.getNodeFromCache(nodeLink.getTargetNodeId());
                Node childNode = children.get(nodeLink);

                // Process all parents of child node with Many-To-One
                // relationship first
                Map<NodeLink, Node> parents = childNode.getParents();
                for (NodeLink parentLink : parents.keySet())
                {
                    Relation.ForeignKey multiplicity = parentLink.getMultiplicity();
                    if (multiplicity.equals(Relation.ForeignKey.MANY_TO_ONE))
                    {
                        Node parentNode = parents.get(parentLink);

                        if (!parentNode.isTraversed() && parentNode.isDirty())
                        {
                            addNodesToFlushStack(parentNode, eventType);
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
                    flushStack.push(childNode);
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
            flushStack.push(node);
            logEvent(node, eventType);
        }

    }

    /**
     * Gets the flush stack.
     * 
     * @return the flushStack
     */
    public FlushStack getFlushStack()
    {
        return flushStack;
    }

    /**
     * Sets the flush stack.
     * 
     * @param flushStack
     *            the flushStack to set
     */
    public void setFlushStack(FlushStack flushStack)
    {
        this.flushStack = flushStack;
    }

    /**
     * Gets the join table data map.
     * 
     * @return the joinTableDataMap
     */
    public Map<String, JoinTableData> getJoinTableDataMap()
    {
        return joinTableDataMap;
    }

    /**
     * Sets the join table data map.
     * 
     * @param joinTableDataMap
     *            the joinTableDataMap to set
     */
    public void setJoinTableDataMap(Map<String, JoinTableData> joinTableDataMap)
    {
        this.joinTableDataMap = joinTableDataMap;
    }

    /**
     * Empties Flush stack present in a PersistenceCache.
     * 
     */
    public void clearFlushStack()
    {
        if (flushStack != null && !flushStack.isEmpty())
        {
            flushStack.clear();
            // flushStack = null;
        }
        if (joinTableDataMap != null && !joinTableDataMap.isEmpty())
        {
            joinTableDataMap.clear();
            // joinTableDataMap = null;
        }

        if (eventLogQueue != null)
        {
            eventLogQueue.clear();
            // eventLogQueue = null;
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
                    log.warn("Caught exception during rollback, Caused by:" + ex.getMessage());
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
                    EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(clazz);
                    Client client = delegator.getClient(metadata);
                    if (node.isProcessed())
                    {
                        if (node.getOriginalNode() == null)
                        {
                            String entityId = ObjectGraphUtils.getEntityId(node.getNodeId());
                            client.delete(node.getData(), entityId);
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
                    log.warn("Caught exception during rollback, Caused by:" + ex.getMessage());
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
    private void addJoinTableDataIntoMap(OPERATION operation, String joinTableName, String joinColumnName,
            String invJoinColumnName, Class<?> entityClass, Object joinColumnValue, Set<Object> invJoinColumnValues)
    {
        JoinTableData joinTableData = joinTableDataMap.get(joinTableName);
        if (joinTableData == null)
        {
            joinTableData = new JoinTableData(operation, joinTableName, joinColumnName, invJoinColumnName, entityClass);
            joinTableData.addJoinTableRecord(joinColumnValue, invJoinColumnValues);
            joinTableDataMap.put(joinTableName, joinTableData);
        }
        else
        {
            joinTableData.addJoinTableRecord(joinColumnValue, invJoinColumnValues);
        }

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
        Map<String, JoinTableData> joinTableDataMap = getJoinTableDataMap();
        for (JoinTableData jtData : joinTableDataMap.values())
        {
            if (jtData.isProcessed())
            {
                EntityMetadata m = KunderaMetadataManager.getEntityMetadata(jtData.getEntityClass());
                Client client = delegator.getClient(m);

                if (OPERATION.INSERT.equals(jtData.getOperation()))
                {
                    for (Object pk : jtData.getJoinTableRecords().keySet())
                    {
//                        client.deleteByColumn(jtData.getJoinTableName(), m.getIdColumn().getName(), pk);
                        client.deleteByColumn(jtData.getJoinTableName(), m.getIdAttribute().getName(), pk);
                    }
                }
                else if (OPERATION.DELETE.equals(jtData.getOperation()))
                {
                    client.persistJoinTable(jtData);
                }
            }
        }
    }

}
