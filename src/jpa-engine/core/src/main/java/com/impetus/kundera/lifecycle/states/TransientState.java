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
import com.impetus.kundera.utils.ObjectUtils;

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

        // Transient ---> Managed
        moveNodeToNextState(nodeStateContext, new ManagedState());

        // Mark this entity for saving in database
        nodeStateContext.setDirty(true);

        // Add this node into persistence cache
        nodeStateContext.getPersistenceCache().getMainCache().addNodeToCache((Node) nodeStateContext);
        // Recurse persist operation on all managed entities for whom
        // cascade=ALL or PERSIST
        // recursivelyPerformOperation(nodeStateContext, OPERATION.PERSIST);
    }

    @Override
    public void handleRemove(NodeStateContext nodeStateContext)
    {
        // Ignored, Entity will remain in the Transient state

        // Recurse remove operation for all related entities for whom
        // cascade=ALL or REMOVE
        // recursivelyPerformOperation(nodeStateContext, OPERATION.REMOVE);
    }

    @Override
    public void handleRefresh(NodeStateContext nodeStateContext)
    {
        throw new IllegalArgumentException("Refresh operation not allowed in Transient state");
    }

    @Override
    public void handleMerge(NodeStateContext nodeStateContext)
    {
        // create a new managed entity and copy state of original entity into
        // this one.
        Object copiedNodeData = ObjectUtils.deepCopy(nodeStateContext.getData(), nodeStateContext.getPersistenceDelegator().getKunderaMetadata());
        nodeStateContext.setData(copiedNodeData);
        moveNodeToNextState(nodeStateContext, new ManagedState());

        nodeStateContext.getPersistenceCache().getMainCache().addNodeToCache((Node) nodeStateContext);
    }

    @Override
    public void handleFind(NodeStateContext nodeStateContext)
    {
        // Nothing to do, Entity once found, jusmps directly to managed state
    }

    @Override
    public void handleClose(NodeStateContext nodeStateContext)
    {
        // Nothing to do, only entities in Managed/ Removed state move to
        // detached state
    }

    @Override
    public void handleClear(NodeStateContext nodeStateContext)
    {
        // Nothing to do, only entities in Managed/ Removed state move to
        // detached state
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
        // Nothing to do, only entities in Managed/ Removed state move to
        // detached state
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
