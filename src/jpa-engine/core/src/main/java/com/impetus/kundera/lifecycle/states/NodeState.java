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
package com.impetus.kundera.lifecycle.states;

import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.NodeStateContext;

/**
 * State machine class for Node state
 * 
 * @author amresh
 * 
 */
public abstract class NodeState
{
    public enum OPERATION
    {
        PERSIST, MERGE, REMOVE, REFRESH, DETACH
    }

    public abstract void initialize(NodeStateContext nodeStateContext);

    // Life cycle Management
    public abstract void handlePersist(NodeStateContext nodeStateContext);

    public abstract void handleRemove(NodeStateContext nodeStateContext);

    public abstract void handleRefresh(NodeStateContext nodeStateContext);

    public abstract void handleMerge(NodeStateContext nodeStateContext);

    public abstract void handleDetach(NodeStateContext nodeStateContext);

    public abstract void handleClose(NodeStateContext nodeStateContext);

    public abstract void handleLock(NodeStateContext nodeStateContext);

    public abstract void handleCommit(NodeStateContext nodeStateContext);

    public abstract void handleRollback(NodeStateContext nodeStateContext);

    // Identity Management
    public abstract void handleFind(NodeStateContext nodeStateContext);

    public abstract void handleGetReference(NodeStateContext nodeStateContext);

    public abstract void handleContains(NodeStateContext nodeStateContext);

    // Cache Management
    public abstract void handleClear(NodeStateContext nodeStateContext);

    public abstract void handleFlush(NodeStateContext nodeStateContext);

    /**
     * @param nodeStateContext
     */
    void moveNodeToNextState(NodeStateContext nodeStateContext, NodeState nextState)
    {
        nodeStateContext.setCurrentNodeState(nextState);
    }

    /**
     * @param nodeStateContext
     */
    void recursivelyPerformOperation(NodeStateContext nodeStateContext, OPERATION operation)
    {
        Map<NodeLink, Node> children = nodeStateContext.getChildren();
        if (children != null)
        {
            for (NodeLink nodeLink : children.keySet())
            {
                List<CascadeType> cascadeTypes = (List<CascadeType>) nodeLink.getLinkProperty(LinkProperty.CASCADE);

                switch (operation)
                {
                /*case PERSIST:
                    if (cascadeTypes.contains(CascadeType.PERSIST) || cascadeTypes.contains(CascadeType.ALL))
                    {
                        Node childNode = children.get(nodeLink);
                        childNode.persist();
                    }
                    break;
                case MERGE:
                    if (cascadeTypes.contains(CascadeType.MERGE) || cascadeTypes.contains(CascadeType.ALL))
                    {
                        Node childNode = children.get(nodeLink);
                        if (childNode.isInState(TransientState.class))
                        {
                            childNode.persist();
                        }
                        else
                        {
                            childNode.merge();
                        }
                    }
                    break;

                case REMOVE:
                    if (cascadeTypes.contains(CascadeType.REMOVE) || cascadeTypes.contains(CascadeType.ALL))
                    {
                        Node childNode = children.get(nodeLink);
                        childNode.remove();
                    }
                    break;*/

                case REFRESH:
                    if (cascadeTypes.contains(CascadeType.REFRESH) || cascadeTypes.contains(CascadeType.ALL))
                    {
                        Node childNode = children.get(nodeLink);
                        childNode.refresh();
                    }
                    break;
                case DETACH:
                    if (cascadeTypes.contains(CascadeType.DETACH) || cascadeTypes.contains(CascadeType.ALL))
                    {
                        Node childNode = children.get(nodeLink);
                        childNode.detach();
                    }
                    break;
                }

            }
        }
    }
}
