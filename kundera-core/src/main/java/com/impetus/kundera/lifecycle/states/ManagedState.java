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
public class ManagedState extends EntityState
{
    @Override
    public void initialize(EntityStateContextImpl context)
    {
    }

    @Override
    public void handlePersist(EntityStateContextImpl context)
    {
        //Ignored, entity remains in the same state
        //TODO: Cascade persist operation for related entities for whom cascade=ALL or PERSIST
    }   

    @Override
    public void handleRemove(EntityStateContextImpl context)
    {
        context.setCurrentEntityState(new RemovedState());
        //TODO: Mark entity for removal in persistence context
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(EntityStateContextImpl context)
    {
        //TODO: Refresh entity state from the database
        //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(EntityStateContextImpl context)
    {
      //Ignored, entity remains in the same state
      //TODO: Cascade manage operation for all related entities for whom cascade=ALL or MERGE
    }
    
    @Override
    public void handleFind(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleClose(EntityStateContextImpl context)
    {
        context.setCurrentEntityState(new DetachedState());
    }

    @Override
    public void handleClear(EntityStateContextImpl context)
    {
    }

    @Override
    public void handleFlush(EntityStateContextImpl context)
    {        
        //TODO: Check for flush mode, if commit do nothing (state will be updated at commit)
        //else if AUTO, synchronize with DB
        
        //Entity state to remain as Managed
        
        //Cascade flush for all related entities for whom cascade=ALL or PERSIST
        
        
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
        context.setCurrentEntityState(new DetachedState());
    }

    @Override
    public void handleRollback(EntityStateContextImpl context)
    {
        //If persistence context is EXTENDED
        context.setCurrentEntityState(new TransientState());
        
        //If persistence context is TRANSACTIONAL
        //context.setCurrentEntityState(new DetachedState());
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
