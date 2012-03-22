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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraph;

/**
 * Base class for all cache required in persistence context 
 * @author amresh.singh
 */
public class CacheBase
{
    private Map<String, Node> nodeMappings;    
    private List<Node> headNodes;
    
    public CacheBase() {
        headNodes = new ArrayList<Node>();
        nodeMappings = new HashMap<String, Node>();
    }
    
    public void addGraphToCache(ObjectGraph graph) {
        getNodeMappings().putAll(graph.getNodeMapping());
        getHeadNodes().add(graph.getHeadNode());
    }
    

    /**
     * @return the nodeMappings
     */
    public Map<String, Node> getNodeMappings()
    {        
        return nodeMappings;
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
    public List<Node> getHeadNodes()
    {
        return headNodes;
    }

    
    public void addHeadNode(Node headNode)
    {        
        headNodes.add(headNode);
    }    
    
}
