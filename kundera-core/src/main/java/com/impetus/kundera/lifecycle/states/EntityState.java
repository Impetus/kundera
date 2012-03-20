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
public abstract class EntityState
{
    public abstract void initialize(EntityStateContextImpl context);
    
    //Life cycle Management
    public abstract void handlePersist(EntityStateContextImpl context);    
    public abstract void handleRemove(EntityStateContextImpl context);
    public abstract void handleRefresh(EntityStateContextImpl context);    
    public abstract void handleMerge(EntityStateContextImpl context);    
    public abstract void handleDetach(EntityStateContextImpl context);
    
    public abstract void handleClose(EntityStateContextImpl context);    
    public abstract void handleLock(EntityStateContextImpl context);    
    
    public abstract void handleCommit(EntityStateContextImpl context);
    public abstract void handleRollback(EntityStateContextImpl context);
    
    //Identity Management
    public abstract void handleFind(EntityStateContextImpl context);
    public abstract void handleGetReference(EntityStateContextImpl context);
    public abstract void handleContains(EntityStateContextImpl context);
    
    //Cache Management
    public abstract void handleClear(EntityStateContextImpl context);
    public abstract void handleFlush(EntityStateContextImpl context);
    
}
