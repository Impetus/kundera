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

import javax.persistence.PersistenceContextType;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.NodeStateContext;

/**
 * @author amresh
 * 
 */
public class RemovedState extends NodeState
{
    @Override
    public void initialize(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handlePersist(NodeStateContext nodeStateContext)
    {
        // Removed ---> Managed State
        moveNodeToNextState(nodeStateContext, new ManagedState());

        // Recurse persist operation on all related entities for whom
        // cascade=ALL or PERSIST
//        recursivelyPerformOperation(nodeStateContext, OPERATION.PERSIST);
    }

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        // Ignored, entity will remain in removed state

        // Recurse remove operation for all related entities for whom
        // cascade=ALL or REMOVE
//        recursivelyPerformOperation(nodeStateContext, OPERATION.REMOVE);
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException("Refresh operation not allowed in Removed state");
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException("Merge operation not allowed in Removed state");
    }

    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
        // Nothing to do, only entities in Managed state move to detached state
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
        // Nothing to do, only entities in Managed state move to detached state
    }

    @Override
    public void handleFlush(NodeStateContext nodeStateContext)
    {
        // Entity state to remain as Removed

        // Flush this node to database
        Client client = nodeStateContext.getClient();

        Node node = (Node) nodeStateContext;

        Object entityId = node.getEntityId();

        client.remove(node.getData(), entityId);

        // Since node is flushed, mark it as NOT dirty
        nodeStateContext.setDirty(false);

        // Remove this node from Persistence Cache
        nodeStateContext.getPersistenceCache().getMainCache().removeNodeFromCache(node);
    }

    @Override
    public void handleLock(NodeStateContext nodeStateContext)
    {
    }

    @Override
    public void handleDetach(NodeStateContext nodeStateContext)
    {
        // Removed ---> Detached
        moveNodeToNextState(nodeStateContext, new DetachedState());

        // Cascade detach operation to all referenced entities for whom
        // cascade=ALL or DETACH
        recursivelyPerformOperation(nodeStateContext, OPERATION.DETACH);
    }

    @Override
    public void handleCommit(NodeStateContext nodeStateContext)
    {
        nodeStateContext.setCurrentNodeState(new TransientState());
    }

    @Override
    public void handleRollback(NodeStateContext nodeStateContext)
    {
        // If persistence context is EXTENDED, Next state should be Managed
        // If persistence context is TRANSACTIONAL, Node should be detached

        if (PersistenceContextType.EXTENDED.equals(nodeStateContext.getPersistenceCache().getPersistenceContextType()))
        {
            moveNodeToNextState(nodeStateContext, new ManagedState());

        }
        else if (PersistenceContextType.TRANSACTION.equals(nodeStateContext.getPersistenceCache()
                .getPersistenceContextType()))
        {
            nodeStateContext.detach();
        }

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
