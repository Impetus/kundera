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


import com.impetus.kundera.lifecycle.NodeStateContext;

/**
 * State machine class for Node state
 * @author amresh
 *
 */
public abstract class NodeState
{   
   
    public abstract void initialize(NodeStateContext nodeStateContext);
    
    //Life cycle Management
    public abstract void handlePersist(NodeStateContext nodeStateContext);    
    public abstract void handleRemove(NodeStateContext nodeStateContext);
    public abstract void handleRefresh(NodeStateContext nodeStateContext);    
    public abstract void handleMerge(NodeStateContext nodeStateContext);    
    public abstract void handleDetach(NodeStateContext nodeStateContext);
    
    public abstract void handleClose(NodeStateContext nodeStateContext);    
    public abstract void handleLock(NodeStateContext nodeStateContext);    
    
    public abstract void handleCommit(NodeStateContext nodeStateContext);
    public abstract void handleRollback(NodeStateContext nodeStateContext);
    
    //Identity Management
    public abstract void handleFind(NodeStateContext nodeStateContext);
    public abstract void handleGetReference(NodeStateContext nodeStateContext);
    public abstract void handleContains(NodeStateContext nodeStateContext);
    
    //Cache Management
    public abstract void handleClear(NodeStateContext nodeStateContext);
    public abstract void handleFlush(NodeStateContext nodeStateContext);   


    public void logStateChangeEvent(NodeState prevState, NodeState nextState, String nodeId) {
        System.out.println("Node: " + nodeId + ":: " + prevState.getClass().getSimpleName() + " >>> " + nextState.getClass().getSimpleName());
    }
    
    public void logNodeEvent(String eventType, NodeState currentState, String nodeId) {
        System.out.println("Node: " + nodeId + ":: " + eventType + " in state " + currentState.getClass().getSimpleName());
    }
    
}
