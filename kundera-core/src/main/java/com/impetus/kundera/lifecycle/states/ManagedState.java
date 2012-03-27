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


import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.NodeStateContext;

/**
 * @author amresh
 *
 */
public class ManagedState extends NodeState
{
    @Override
    public void initialize(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handlePersist(NodeStateContext nodeStateContext)
    {
        //Ignored, entity remains in the same state
        //TODO: Cascade persist operation for related entities for whom cascade=ALL or PERSIST
    }   

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        nodeStateContext.setCurrentNodeState(new RemovedState());
        //TODO: Mark entity for removal in persistence context
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        //TODO: Refresh entity state from the database
        //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
      //Ignored, entity remains in the same state
      //TODO: Cascade manage operation for all related entities for whom cascade=ALL or MERGE
    }
    
    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
        nodeStateContext.setCurrentNodeState(new DetachedState());
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleFlush(NodeStateContext nodeStateContext)
    {        
        //Entity state to remain as Managed     
        
        //Flush this node to database      
        Client client = nodeStateContext.getClient();
        client.persist((Node)nodeStateContext);     
        
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
        nodeStateContext.setCurrentNodeState(new DetachedState());
    }

    @Override
    public void handleRollback(NodeStateContext nodeStateContext)
    {
        //If persistence context is EXTENDED
        nodeStateContext.setCurrentNodeState(new TransientState());
        
        //If persistence context is TRANSACTIONAL
        //context.setCurrentEntityState(new DetachedState());
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
