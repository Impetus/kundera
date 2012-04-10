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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.graph.ObjectGraphBuilder;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.RemovedState;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

/**
 * Provides utility methods for managing Flush Stack.
 * 
 * @author amresh.singh
 */
public class FlushManager
{
    public void buildFlushStack(PersistenceCache pc)
    {

        // Process Main cache
        MainCache mainCache = (MainCache) pc.getMainCache();
        new PersistenceCacheManager(pc).markAllNodesNotTraversed();

        Set<Node> headNodes = mainCache.getHeadNodes();

        for (Node headNode : headNodes)
        {
            if(headNode.isDirty())
            addNodesToFlushStack(pc, headNode);

        }
    }

    /**
     * Pushes <code>node</code> and its descendants recursively to flush stack
     * residing into persistence cache
     * 
     * @param pc
     * @param node
     */
    public void addNodesToFlushStack(PersistenceCache pc, Node node)
    {
        FlushStack flushStack = pc.getFlushStack();
        MainCache mainCache = (MainCache) pc.getMainCache();

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
                Node childNode = mainCache.getNodeFromCache(nodeLink.getTargetNodeId());

                if (!childNode.isTraversed())
                {
                    addNodesToFlushStack(pc, childNode);
                }

            }

            // Process Many-To-Many children
            for (NodeLink nodeLink : manyToManyChildren.keySet())
            {
                Node childNode = mainCache.getNodeFromCache(nodeLink.getTargetNodeId());

                // Extract information required to be persisted into Join Table
                if (node.isDirty() && !node.isTraversed())
                {
                    JoinTableMetadata jtmd = (JoinTableMetadata) nodeLink
                            .getLinkProperty(LinkProperty.JOIN_TABLE_METADATA);
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

                    pc.addJoinTableDataIntoMap(operation, jtmd.getJoinTableName(), joinColumnName,
                            inverseJoinColumnName, node.getDataClass(), entityId, childValues);

                }

                // Process child node Graph recursively first
                if (!childNode.isTraversed())
                {
                    addNodesToFlushStack(pc, childNode);
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

                    // Process child node Graph recursively
                    Node childNode = mainCache.getNodeFromCache(nodeLink.getTargetNodeId());
                    addNodesToFlushStack(pc, childNode);
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
                }

                // Child node of this node
                Node childNode = mainCache.getNodeFromCache(nodeLink.getTargetNodeId());

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
                            addNodesToFlushStack(pc, parentNode);
                        }
                    }
                }

                // Finally process this child node
                if (!childNode.isTraversed() && !childNode.isDirty())
                {
                    addNodesToFlushStack(pc, childNode);
                } else
                {
                    childNode.setTraversed(true);
                    flushStack.push(childNode);
                }
            }
        }

        // Finally, if this node itself is not traversed yet, (as may happen in
        // 1-1 and M-1
        // cases), push it to stack
        if (!node.isTraversed() && node.isDirty())
        {
            node.setTraversed(true);
            flushStack.push(node);
        }

    }

    /**
     * Empties Flush stack present in a PersistenceCache
     * 
     * @param pc
     *            Persistence Cache holding flush stack
     */
    public void clearFlushStack(PersistenceCache pc)
    {
        FlushStack flushStack = pc.getFlushStack();

        if (flushStack != null && !flushStack.isEmpty())
        {
            flushStack.clear();
        }
    }
}
