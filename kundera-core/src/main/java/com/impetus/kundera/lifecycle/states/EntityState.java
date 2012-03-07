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
    //Life cycle Management
    public abstract void persist(EntityStateManagerImpl context);    
    public abstract void remove(EntityStateManagerImpl context);
    public abstract void refresh(EntityStateManagerImpl context);    
    public abstract void merge(EntityStateManagerImpl context);    
    public abstract void detach(EntityStateManagerImpl context);
    
    public abstract void close(EntityStateManagerImpl context);    
    public abstract void lock(EntityStateManagerImpl context);    
    
    public abstract void commit(EntityStateManagerImpl context);
    public abstract void rollback(EntityStateManagerImpl context);
    
    //Identity Management
    public abstract void find(EntityStateManagerImpl context);
    public abstract void getReference(EntityStateManagerImpl context);
    public abstract void contains(EntityStateManagerImpl context);
    
    //Cache Management
    public abstract void clear(EntityStateManagerImpl context);
    public abstract void flush(EntityStateManagerImpl context);
    
}
