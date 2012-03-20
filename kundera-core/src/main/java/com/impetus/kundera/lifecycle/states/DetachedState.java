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

import com.impetus.kundera.lifecycle.EntityStateContextImpl;

/**
 * @author amresh
 *
 */
public class DetachedState extends EntityState
{
    @Override
    public void initialize(EntityStateContextImpl context)
    {
    }

    @Override
    public void handlePersist(EntityStateContextImpl context)
    {
        throw new IllegalArgumentException("Persist operation not allowed in Detached state");
    }   

    @Override
    public void handleRemove(EntityStateContextImpl context)
    {
        throw new IllegalArgumentException("Remove operation not allowed in Detached state");
    }

    @Override
    public void handleRefresh(EntityStateContextImpl context)
    {
        throw new IllegalArgumentException("Refresh operation not allowed in Detached state");
    }

    @Override
    public void handleMerge(EntityStateContextImpl context)
    {
        context.setCurrentEntityState(new ManagedState());
        //TODO: Copy detached entity's current state to existing managed instance of the 
        // same entity identity (if one exists), or create a new managed copy
        //TODO: Cascade manage operation for all related entities for whom cascade=ALL or MERGE
    }
    
    @Override
    public void handleFind(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleClose(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleClear(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleFlush(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleLock(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleDetach(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleCommit(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleRollback(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleGetReference(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleContains(EntityStateContextImpl context)
    {
    } 
    
    

}
