/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.graph;

import java.util.HashMap;
import java.util.Map;


import com.impetus.kundera.client.Client;
import com.impetus.kundera.lifecycle.NodeStateContext;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.lifecycle.states.TransientState;

/**
 * Represents a node in object graph 
 * @author amresh.singh
 */
public class Node implements NodeStateContext
{  
    
    //ID of a node into object graph
    private String nodeId;
    
    //Actual node data
    private Object data;
    
    //Current node state as defined in state machine
    private NodeState currentNodeState;
    
    //Class of actual node data
    private Class<?> dataClass;    
    
    //All parents of this node, Key is Node Link info and value is node itself
    private Map<NodeLink, Node> parents;
    
    //All children of this node, Key is Node Link info and value is node itself
    private Map<NodeLink, Node> children;
    
    //Whether this node has been traversed
    private boolean traversed;
    
    //Whether this node is dirty
    private boolean dirty;
    
    //Whether this is a head node
    private boolean isHeadNode;
    
    /** Client for this node */
    Client client;
    
    public Node() {        
    }
    
    public Node(String nodeId, Object data) {
        initializeNode(nodeId, data);    
        
        //Initialize current node state to transient state
        this.currentNodeState = new TransientState();
    }
    
    public Node(String nodeId, Object data, NodeState initialNodeState) {
        initializeNode(nodeId, data);
        
        //Initialize current node state
        this.currentNodeState = initialNodeState;
    }
    
    public Node(String nodeId, Class<?> nodeDataClass, NodeState initialNodeState) {
        this.nodeId = nodeId;
        this.dataClass = nodeDataClass;
        
        //Initialize current node state
        this.currentNodeState = initialNodeState;
    }
    
    private void initializeNode(String nodeId, Object data) {
        this.nodeId = nodeId;
        this.data = data;
        this.dataClass = data.getClass();
    }

    /**
     * @return the nodeId
     */
    @Override
    public String getNodeId()
    {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    @Override
    public void setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
    }

    /**
     * @return the data
     */
    @Override
    public Object getData()
    {
        return data;
    }

    /**
     * @param data the data to set
     */
    @Override
    public void setData(Object data)
    {
        this.data = data;
    }

    /**
     * @return the dataClass
     */
    @Override
    public Class getDataClass()
    {
        return dataClass;
    }    
    
    /**
     * @param dataClass the dataClass to set
     */
    @Override
    public void setDataClass(Class dataClass)
    {
        this.dataClass = dataClass;
    }

    /**
     * @return the currentNodeState
     */
    @Override
    public NodeState getCurrentNodeState()
    {
        return currentNodeState;
    }

    /**
     * @param currentNodeState the currentNodeState to set
     */
    @Override
    public void setCurrentNodeState(NodeState currentNodeState)
    {
        this.currentNodeState = currentNodeState;
    }    

    /**
     * @return the parents
     */
    @Override
    public Map<NodeLink, Node> getParents()
    {
        return parents;
    }

    /**
     * @param parents the parents to set
     */
    @Override
    public void setParents(Map<NodeLink, Node> parents)
    {
        this.parents = parents;
    }

    /**
     * @return the children
     */
    @Override
    public Map<NodeLink, Node> getChildren()
    {
        return children;
    }

    /**
     * @param children the children to set
     */
    @Override
    public void setChildren(Map<NodeLink, Node> children)
    {
        this.children = children;
    }  
    
    
    /**
     * @return the isHeadNode
     */
    public boolean isHeadNode()
    {
        return isHeadNode;
    }

    /**
     * @param isHeadNode the isHeadNode to set
     */
    public void setHeadNode(boolean isHeadNode)
    {
        this.isHeadNode = isHeadNode;
    }

    /**
     * Retrieves parent node of this node for a given parent node ID
     */
    @Override
    public Node getParentNode(String parentNodeId) {
        NodeLink link = new NodeLink(parentNodeId, getNodeId());
        
        if(this.parents == null) {
            return null;
        } else {
            return this.parents.get(link);
        }
    }
    
    /**
     * Retrieves child node of this node for a given child node ID
     */
    
    @Override
    public Node getChildNode(String childNodeId) {
        NodeLink link = new NodeLink(getNodeId(), childNodeId);
        
        if(this.children == null) {
            return null;
        } else {
            return this.children.get(link);
        }
    }
    
    @Override
    public void addParentNode(NodeLink nodeLink, Node node) {
        if(parents == null || parents.isEmpty()) {
            parents = new HashMap<NodeLink, Node>();
        }
        parents.put(nodeLink, node);
    }
    
    @Override
    public void addChildNode(NodeLink nodeLink, Node node) {
        if(children == null || children.isEmpty()) {
            children = new HashMap<NodeLink, Node>();
        }
        children.put(nodeLink, node);
    }   
    
    /**
     * @return the traversed
     */
    @Override
    public boolean isTraversed()
    {
        return traversed;
    }

    /**
     * @param traversed the traversed to set
     */
    @Override
    public void setTraversed(boolean traversed)
    {
        this.traversed = traversed;
    }   

    /**
     * @return the dirty
     */
    @Override
    public boolean isDirty()
    {
        return dirty;
    }

    /**
     * @param dirty the dirty to set
     */
    @Override
    public void setDirty(boolean dirty)
    {
        this.dirty = dirty;
    }  
    

    /**
     * @return the client
     */
    public Client getClient()
    {
        return client;
    }

    /**
     * @param client the client to set
     */
    public void setClient(Client client)
    {
        this.client = client;
    }

    @Override
    public String toString() {
        return "[" + nodeId +  "]";
    } 
    
    
    //////////////////////////////////////////
    /* CRUD related operations on this node */
    //////////////////////////////////////////
    
    @Override
    public void persist()
    {
        getCurrentNodeState().handlePersist(this);
    }

    @Override
    public void remove()
    {
        getCurrentNodeState().handleRemove(this);
    }

    @Override
    public void refresh()
    {
        getCurrentNodeState().handleRefresh(this);
    }

    @Override
    public void merge()
    {
        getCurrentNodeState().handleMerge(this);
    }

    @Override
    public void detach()
    {
        getCurrentNodeState().handleDetach(this);
    }

    @Override
    public void close()
    {
        getCurrentNodeState().handleClose(this);
    }

    @Override
    public void lock()
    {
        getCurrentNodeState().handleLock(this);
    }

    @Override
    public void commit()
    {
        getCurrentNodeState().handleCommit(this);
    }

    @Override
    public void rollback()
    {
        getCurrentNodeState().handleRollback(this);
    }

    @Override
    public void find()
    {
        getCurrentNodeState().handleFind(this);
    }

    @Override
    public void getReference()
    {
        getCurrentNodeState().handleGetReference(this);
    }

    @Override
    public void contains()
    {
        getCurrentNodeState().handleContains(this);
    }

    @Override
    public void clear()
    {
        getCurrentNodeState().handleClear(this);
    }

    @Override
    public void flush()
    {
        getCurrentNodeState().handleFlush(this);
    }   

}
