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

import com.impetus.kundera.graph.Node;

/**
 * State machine class for Node state
 * @author amresh
 *
 */
public abstract class NodeState
{
    public abstract void initialize(Node node);
    
    //Life cycle Management
    public abstract void handlePersist(Node node);    
    public abstract void handleRemove(Node node);
    public abstract void handleRefresh(Node node);    
    public abstract void handleMerge(Node node);    
    public abstract void handleDetach(Node node);
    
    public abstract void handleClose(Node node);    
    public abstract void handleLock(Node node);    
    
    public abstract void handleCommit(Node node);
    public abstract void handleRollback(Node node);
    
    //Identity Management
    public abstract void handleFind(Node node);
    public abstract void handleGetReference(Node node);
    public abstract void handleContains(Node node);
    
    //Cache Management
    public abstract void handleClear(Node node);
    public abstract void handleFlush(Node node);
    
}
