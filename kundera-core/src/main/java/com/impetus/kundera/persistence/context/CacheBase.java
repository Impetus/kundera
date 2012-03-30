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
package com.impetus.kundera.persistence.context;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.ObjectGraph;

/**
 * Base class for all cache required in persistence context 
 * @author amresh.singh
 */
public class CacheBase
{
    private Map<String, Node> nodeMappings;    
    private Set<Node> headNodes;
    
    public CacheBase() {
        headNodes = new HashSet<Node>();
        nodeMappings = new HashMap<String, Node>();
    }
    
    public Node getNodeFromCache(String nodeId) {    
        
        Node node = nodeMappings.get(nodeId);
        
        if(node != null) {
            logCacheEvent("FETCHED FROM ", nodeId);
        }        
        return node;
    }
    
    public void addNodeToCache(Node node) {
       /* check if this node already exists in cache node mappings
        * If yes, update parents and children links
        * Otherwise, just simply add the node to cache node mappings
       */
       
        if (nodeMappings.containsKey(node.getNodeId()))
        {            
            Node existingNode = nodeMappings.get(node.getNodeId());

            if (existingNode.getParents() != null)
            {
                if (node.getParents() == null)
                {
                    node.setParents(new HashMap<NodeLink, Node>());
                }
                node.getParents().putAll(existingNode.getParents());
            }

            if (existingNode.getChildren() != null)
            {
                if (node.getChildren() == null)
                {
                    node.setChildren(new HashMap<NodeLink, Node>());
                }
                node.getChildren().putAll(existingNode.getChildren());
            }
            
            logCacheEvent("ADDED TO ", node.getNodeId());
            nodeMappings.put(node.getNodeId(), node);
        }
        else
        {
            logCacheEvent("ADDED TO ", node.getNodeId());
            nodeMappings.put(node.getNodeId(), node);
        }
    }   
    
    public void addGraphToCache(ObjectGraph graph) {
        
        //Add head Node to list of head nodes
        getHeadNodes().add(graph.getHeadNode());
        
        //Add each node in the graph to cache
        for(String key : graph.getNodeMapping().keySet()) {            
            Node thisNode = graph.getNodeMapping().get(key);
            addNodeToCache(thisNode);            
        }
        
    }
    
    private void logCacheEvent(String eventType, String nodeId) {
        System.out.println("Node: " + nodeId + ":: " + eventType + " PC");
    }    

    /**
     * @param nodeMappings the nodeMappings to set
     */
    public void setNodeMappings(Map<String, Node> nodeMappings)
    {
        this.nodeMappings = nodeMappings;
    }

    /**
     * @return the headNodes
     */
    public Set<Node> getHeadNodes()
    {
        return headNodes;
    }

    
    public void addHeadNode(Node headNode)
    {        
        headNodes.add(headNode);
    }    
    
    public int size() {
        return nodeMappings.size();
    }
    
    public Collection<Node> getAllNodes() {
        return nodeMappings.values();
    }
    
}
