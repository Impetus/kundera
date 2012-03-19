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
public abstract class EntityState
{
    public abstract void initialize(EntityStateManagerImpl context);
    
    //Life cycle Management
    public abstract void handlePersist(EntityStateManagerImpl context);    
    public abstract void handleRemove(EntityStateManagerImpl context);
    public abstract void handleRefresh(EntityStateManagerImpl context);    
    public abstract void handleMerge(EntityStateManagerImpl context);    
    public abstract void handleDetach(EntityStateManagerImpl context);
    
    public abstract void handleClose(EntityStateManagerImpl context);    
    public abstract void handleLock(EntityStateManagerImpl context);    
    
    public abstract void handleCommit(EntityStateManagerImpl context);
    public abstract void handleRollback(EntityStateManagerImpl context);
    
    //Identity Management
    public abstract void handleFind(EntityStateManagerImpl context);
    public abstract void handleGetReference(EntityStateManagerImpl context);
    public abstract void handleContains(EntityStateManagerImpl context);
    
    //Cache Management
    public abstract void handleClear(EntityStateManagerImpl context);
    public abstract void handleFlush(EntityStateManagerImpl context);
    
}
