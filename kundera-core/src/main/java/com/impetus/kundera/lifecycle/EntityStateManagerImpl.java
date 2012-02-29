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
 * @author amresh
 *
 */
public class EntityStateManagerImpl implements EntityStateManager
{
    private EntityState currentEntityState;  
    
    
    public EntityStateManagerImpl(EntityState entityState) {
        this.currentEntityState = entityState;
    }
    
    public EntityStateManagerImpl() {
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
     * @param currentEntityState the currentEntityState to set
     */
    public void setCurrentEntityState(EntityState currentEntityState)
    {
        this.currentEntityState = currentEntityState;
    }
    
}
