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

import com.impetus.kundera.lifecycle.EntityStateManagerImpl;

/**
 * @author amresh
 *
 */
public class TransientState extends EntityState
{    

    @Override
    public void initialize(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handlePersist(EntityStateManagerImpl context)
    {
        context.setCurrentEntityState(new ManagedState());
        //TODO: Mark this entity for saving in database
        //TODO: Recurse persist operation on all managed entities for whom cascade=ALL or PERSIST
    }   

    @Override
    public void handleRemove(EntityStateManagerImpl context)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Recurse remove operation for all related entities for whom cascade=ALL or REMOVE
    }

    @Override
    public void handleRefresh(EntityStateManagerImpl context)
    {
        //Ignored, Entity will remain in the Transient state
        //TODO: Cascade refresh operation for all related entities for whom cascade=ALL or REFRESH
    }

    @Override
    public void handleMerge(EntityStateManagerImpl context)
    {
        //TODO: create a new managed entity and copy state of original entity into this one.
    }
    
    @Override
    public void handleFind(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleClose(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleClear(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleFlush(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleLock(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleDetach(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleCommit(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleRollback(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleGetReference(EntityStateManagerImpl context)
    {
    }

    @Override
    public void handleContains(EntityStateManagerImpl context)
    {
    }    
    
}
