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

/**
 * @author amresh
 *
 */
public class TransientState extends NodeState
{    

    @Override
    public void initialize(Node node)
    {
    }

    @Override
    public void handlePersist(Node node)
    {
        //Transient ---> Managed
        node.setCurrentEntityState(new ManagedState());      
        
        //Mark this entity for saving in database
        node.setDirty(true);
        
        //Add this node into persistence cache

        //Recurse persist operation on all managed entities for whom cascade=ALL or PERSIST
        Map<NodeLink, Node> children = node.getChildren();
        for(NodeLink nodeLink : children.keySet()) {
            List<CascadeType> cascadeTypes = (List<CascadeType>) nodeLink.getLinkProperty(LinkProperty.CASCADE);
            if(cascadeTypes.contains(CascadeType.PERSIST) || cascadeTypes.contains(CascadeType.ALL)) {
                Node childNode = children.get(nodeLink);                
                childNode.persist();
            }
        }
    }                
                   

    @Override
    public void handleRemove(Node node)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(Node node)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(Node node)
    {
        //TODO: create a new managed entity and copy state of original entity into this one.
    }
    
    @Override
    public void handleFind(Node node)
    {
    }

    @Override
    public void handleClose(Node node)
    {
    }

    @Override
    public void handleClear(Node node)
    {
    }

    @Override
    public void handleFlush(Node node)
    {
    }

    @Override
    public void handleLock(Node node)
    {
    }

    @Override
    public void handleDetach(Node node)
    {
    }

    @Override
    public void handleCommit(Node node)
    {
    }

    @Override
    public void handleRollback(Node node)
    {
    }

    @Override
    public void handleGetReference(Node node)
    {
    }

    @Override
    public void handleContains(Node node)
    {
    }    
    
}
