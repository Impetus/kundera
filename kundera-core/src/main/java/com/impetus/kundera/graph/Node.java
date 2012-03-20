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

import com.impetus.kundera.lifecycle.EntityStateContextImpl;

/**
 * Represents a node in object graph 
 * @author amresh.singh
 */
public class Node extends EntityStateContextImpl
{
    //ID of a node into object graph
    private String nodeId;
    
    //Actual node data
    private Object data;
    
    //Class of actual node data
    private Class<?> dataClass;    
    
    //All parents of this node, Key is Node Link info and value is node itself
    private Map<NodeLink, Node> parents;
    
    //All children of this node, Key is Node Link info and value is node itself
    private Map<NodeLink, Node> children;
    
    //Whether this node has been traversed
    private boolean traversed;
    
    public Node() {
        
    }
    
    public Node(String nodeId, Object data) {
        this.nodeId = nodeId;
        this.data = data;
        this.dataClass = data.getClass();
    }

    /**
     * @return the nodeId
     */
    public String getNodeId()
    {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
    }

    /**
     * @return the data
     */
    public Object getData()
    {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(Object data)
    {
        this.data = data;
    }

    /**
     * @return the dataClass
     */
    public Class getDataClass()
    {
        return dataClass;
    }

    /**
     * @param dataClass the dataClass to set
     */
    public void setDataClass(Class dataClass)
    {
        this.dataClass = dataClass;
    }

    /**
     * @return the parents
     */
    public Map<NodeLink, Node> getParents()
    {
        return parents;
    }

    /**
     * @param parents the parents to set
     */
    public void setParents(Map<NodeLink, Node> parents)
    {
        this.parents = parents;
    }

    /**
     * @return the children
     */
    public Map<NodeLink, Node> getChildren()
    {
        return children;
    }

    /**
     * @param children the children to set
     */
    public void setChildren(Map<NodeLink, Node> children)
    {
        this.children = children;
    }
    
    public Node getParentNode(String parentNodeId) {
        NodeLink link = new NodeLink(parentNodeId, getNodeId());
        
        if(this.parents == null) {
            return null;
        } else {
            return this.parents.get(link);
        }
    }
    
    public Node getChildNode(String childNodeId) {
        NodeLink link = new NodeLink(getNodeId(), childNodeId);
        
        if(this.children == null) {
            return null;
        } else {
            return this.children.get(link);
        }
    }
    
    public void addParentNode(NodeLink nodeLink, Node node) {
        if(parents == null || parents.isEmpty()) {
            parents = new HashMap<NodeLink, Node>();
        }
        parents.put(nodeLink, node);
    }
    
    public void addChildNode(NodeLink nodeLink, Node node) {
        if(children == null || children.isEmpty()) {
            children = new HashMap<NodeLink, Node>();
        }
        children.put(nodeLink, node);
    }   
    
    /**
     * @return the traversed
     */
    public boolean isTraversed()
    {
        return traversed;
    }

    /**
     * @param traversed the traversed to set
     */
    public void setTraversed(boolean traversed)
    {
        this.traversed = traversed;
    }

    @Override
    public String toString() {
        return "[" + nodeId + "," + data.getClass().getSimpleName() + "]";
    }   

}
