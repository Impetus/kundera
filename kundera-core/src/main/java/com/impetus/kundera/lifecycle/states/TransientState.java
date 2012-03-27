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
import com.impetus.kundera.persistence.context.PersistenceCache;

/**
 * @author amresh
 *
 */
public class TransientState extends NodeState
{    

    @Override
    public void initialize(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handlePersist(NodeStateContext nodeStateContext)
    {        
        
        //Transient ---> Managed
        nodeStateContext.setCurrentNodeState(new ManagedState());      
        
        //Mark this entity for saving in database
        nodeStateContext.setDirty(true);
        
        //If it's a head node, add this to the list of head nodes in PC
        if(nodeStateContext.isHeadNode()) {
            PersistenceCache.INSTANCE.getMainCache().addHeadNode((Node)nodeStateContext);
        }
        
        //Add this node into persistence cache
        PersistenceCache.INSTANCE.getMainCache().addNodeToCache((Node)nodeStateContext);

        //Recurse persist operation on all managed entities for whom cascade=ALL or PERSIST
        Map<NodeLink, Node> children = nodeStateContext.getChildren();
        if(children != null) {
            for(NodeLink nodeLink : children.keySet()) {
                List<CascadeType> cascadeTypes = (List<CascadeType>) nodeLink.getLinkProperty(LinkProperty.CASCADE);
                if(cascadeTypes.contains(CascadeType.PERSIST) || cascadeTypes.contains(CascadeType.ALL)) {
                    Node childNode = children.get(nodeLink);                
                    childNode.persist();
                }
            }
        }
        
    }                
                   

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
        //TODO: create a new managed entity and copy state of original entity into this one.
    }
    
    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleFlush(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleLock(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleDetach(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleCommit(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleRollback(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleGetReference(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleContains(NodeStateContext nodeStateContext)
    {
    }    
    
}
