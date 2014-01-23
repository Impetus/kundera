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
import com.impetus.kundera.lifecycle.NodeStateContext;

/**
 * @author amresh
 * 
 */
public class DetachedState extends NodeState
{

    @Override
    public void initialize(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handlePersist(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException("Persist operation not allowed in Detached state");
    }

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException(
                "Remove operation not allowed in Detached state."
                        + " Possible reason: You may have closed entity manager before calling remove. A solution is to call merge before remove.");
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException("Refresh operation not allowed in Detached state");
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
        // Detached ---> Managed
        moveNodeToNextState(nodeStateContext, new ManagedState());

        ((Node) nodeStateContext).setUpdate(true);

        // Copy detached entity's current state to existing managed instance of
        // the
        // same entity identity (if one exists), or create a new managed copy

        // Cascade manage operation for all related entities for whom
        // cascade=ALL or MERGE
//        recursivelyPerformOperation(nodeStateContext, OPERATION.MERGE);
    }

    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
        // Nothing to do, already in Detached State
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
        // Nothing to do, already in Detached State
    }

    @Override
    public void handleFlush(NodeStateContext nodeStateContext)
    {
        // Nothing to do, Entities are flushed from Managed/ Removed state only
    }

    @Override
    public void handleLock(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleDetach(NodeStateContext nodeStateContext)
    {
        // Nothing to do, already in Detached State
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
