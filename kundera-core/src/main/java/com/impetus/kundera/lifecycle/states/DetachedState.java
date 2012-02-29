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
public class DetachedState extends EntityState
{

    @Override
    public void persist(EntityStateManagerImpl context)
    {
        throw new IllegalArgumentException("Persist operation not allowed in Detached state");
    }   

    @Override
    public void remove(EntityStateManagerImpl context)
    {
        throw new IllegalArgumentException("Remove operation not allowed in Detached state");
    }

    @Override
    public void refresh(EntityStateManagerImpl context)
    {
        throw new IllegalArgumentException("Refresh operation not allowed in Detached state");
    }

    @Override
    public void merge(EntityStateManagerImpl context)
    {
        context.setCurrentEntityState(new ManagedState());
        //TODO: Copy detached entity's current state to existing managed instance of the 
        // same entity identity (if one exists), or create a new manaed copy
    }
    
    @Override
    public void find(EntityStateManagerImpl context)
    {
    }

    @Override
    public void close(EntityStateManagerImpl context)
    {
    }

    @Override
    public void clear(EntityStateManagerImpl context)
    {
    }

    @Override
    public void flush(EntityStateManagerImpl context)
    {
    }

    @Override
    public void lock(EntityStateManagerImpl context)
    {
    }

    @Override
    public void detach(EntityStateManagerImpl context)
    {
    }

    @Override
    public void commit(EntityStateManagerImpl context)
    {
    }

    @Override
    public void rollback(EntityStateManagerImpl context)
    {
    }
    

}
