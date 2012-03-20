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

import com.impetus.kundera.lifecycle.states.EntityState;
import com.impetus.kundera.lifecycle.states.TransientState;

/**
 * 
 * @author amresh
 * 
 */
public class EntityStateContextImpl implements EntityStateContext
{
    private EntityState currentEntityState;

    public EntityStateContextImpl(EntityState entityState)
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
    public EntityState getCurrentEntityState()
    {
        return currentEntityState;
    }

    /**
     * @param currentEntityState
     *            the currentEntityState to set
     */
    public void setCurrentEntityState(EntityState currentEntityState)
    {
        this.currentEntityState = currentEntityState;
    }

    @Override
    public void persist()
    {
        getCurrentEntityState().handlePersist(this);
    }

    @Override
    public void remove()
    {
        getCurrentEntityState().handleRemove(this);
    }

    @Override
    public void refresh()
    {
    }

    @Override
    public void merge()
    {
    }

    @Override
    public void detach()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public void lock()
    {
    }

    @Override
    public void commit()
    {
    }

    @Override
    public void rollback()
    {
    }

    @Override
    public void find()
    {
    }

    @Override
    public void getReference()
    {
    }

    @Override
    public void contains()
    {
    }

    @Override
    public void clear()
    {
    }

    @Override
    public void flush()
    {
    }   

}
