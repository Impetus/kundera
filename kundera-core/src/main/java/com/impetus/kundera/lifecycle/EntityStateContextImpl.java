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
package com.impetus.kundera.lifecycle;

import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.lifecycle.states.TransientState;

/**
 * 
 * @author amresh
 * 
 */
public class EntityStateContextImpl implements EntityStateContext
{
    private NodeState currentEntityState;

    public EntityStateContextImpl(NodeState entityState)
    {
        this.currentEntityState = entityState;
    }

    public EntityStateContextImpl()
    {        
        this(new TransientState());       
    }

    /**
     * @return the currentEntityState
     */
    public NodeState getCurrentEntityState()
    {
        return currentEntityState;
    }

    /**
     * @param currentEntityState
     *            the currentEntityState to set
     */
    public void setCurrentEntityState(NodeState currentEntityState)
    {
        this.currentEntityState = currentEntityState;
    }   

    @Override
    public void persist()
    {
        getCurrentEntityState().handlePersist(null);
    }

    @Override
    public void remove()
    {
        getCurrentEntityState().handleRemove(null);
    }

    @Override
    public void refresh()
    {
        getCurrentEntityState().handleRefresh(null);
    }

    @Override
    public void merge()
    {
        getCurrentEntityState().handleMerge(null);
    }

    @Override
    public void detach()
    {
        getCurrentEntityState().handleDetach(null);
    }

    @Override
    public void close()
    {
        getCurrentEntityState().handleClose(null);
    }

    @Override
    public void lock()
    {
        getCurrentEntityState().handleLock(null);
    }

    @Override
    public void commit()
    {
        getCurrentEntityState().handleCommit(null);
    }

    @Override
    public void rollback()
    {
        getCurrentEntityState().handleRollback(null);
    }

    @Override
    public void find()
    {
        getCurrentEntityState().handleFind(null);
    }

    @Override
    public void getReference()
    {
        getCurrentEntityState().handleGetReference(null);
    }

    @Override
    public void contains()
    {
        getCurrentEntityState().handleContains(null);
    }

    @Override
    public void clear()
    {
        getCurrentEntityState().handleClear(null);
    }

    @Override
    public void flush()
    {
        getCurrentEntityState().handleFlush(null);
    }   

}
