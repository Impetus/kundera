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

/**
 * Holds graph of an object
 * 
 * @author amresh.singh
 */
public class ObjectGraph
{
    // Head node in this object graph
    private Node headNode;

    // Mapping between Node ID and Node itself
    // Each node contains link to parent/ child nodes it is related to
    private Map<String, Node> nodeMapping;

    ObjectGraph()
    {
        clear();
        nodeMapping = new HashMap<String, Node>();
    }

    /**
     * Adds a {@link Node} with a give nodeId to object graph.
     * 
     * @param nodeId
     * @param node
     */
    public void addNode(String nodeId, Node node)
    {
        nodeMapping.put(nodeId, node);
    }

    /**
     * Returns Node for a given node ID
     * 
     * @param nodeId
     * @return
     */
    Node getNode(String nodeId)
    {
        return nodeMapping.get(nodeId);
    }

    /**
     * @return the headNode
     */
    public Node getHeadNode()
    {
        return headNode;
    }

    /**
     * @param headNode
     *            the headNode to set
     */
    void setHeadNode(Node headNode)
    {
        this.headNode = headNode;
    }

    /**
     * @return the nodeMapping
     */
    public Map<String, Node> getNodeMapping()
    {
        return nodeMapping;
    }

    public void clear()
    {
        if (nodeMapping != null)
        {
            nodeMapping.clear();
            nodeMapping = null;
        }
    }
}
