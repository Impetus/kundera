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

import com.impetus.kundera.graph.Node;

/**
 * @author amresh
 *
 */
public class RemovedState extends NodeState
{
    @Override
    public void initialize(Node node)
    {
    }

    @Override
    public void handlePersist(Node node)
    {
        node.setCurrentEntityState(new ManagedState());
        //TODO: Recurse persist operation on all related entities for whom cascade=ALL or PERSIST
    }    

    @Override
    public void handleRemove(Node node)
    {
        //Ignored, entity will remain in removed state
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(Node node)
    {
      //Ignored, entity will remain in removed state
      //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(Node node)
    {
        throw new IllegalArgumentException("Merge operation not allowed in Removed state");
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
        node.setCurrentEntityState(new TransientState());
    }

    @Override
    public void handleRollback(Node node)
    {
        //If Persistence Context is EXTENDED
        node.setCurrentEntityState(new ManagedState());
        
        //If Persistence Context is TRANSACTIONAL
        //context.setCurrentEntityState(new DetachedState());
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
