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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.NodeLink;
import com.impetus.kundera.graph.ObjectGraph;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.ObjectUtils;

/**
 * Base class for all cache required in persistence context
 * 
 * @author amresh.singh
 */
public class CacheBase
{
    private static Logger log = LoggerFactory.getLogger(CacheBase.class);

    private Map<String, Node> nodeMappings;

    private Set<Node> headNodes;

    private com.impetus.kundera.cache.Cache l2Cache;

    private PersistenceCache persistenceCache;

    public CacheBase(com.impetus.kundera.cache.Cache l2Cache, PersistenceCache pc)
    {
        this.headNodes = new HashSet<Node>();
        this.nodeMappings = new ConcurrentHashMap<String, Node>();
        this.l2Cache = l2Cache;
        this.persistenceCache = pc;
    }

    public Node getNodeFromCache(String nodeId, PersistenceDelegator pd)
    {
        Node node = nodeMappings.get(nodeId);
        // if not present in first level cache, check from second level cache.
        return node != null ? node : lookupL2Cache(nodeId, pd);
    }

    public Node getNodeFromCache(Object entity, EntityMetadata entityMetadata, PersistenceDelegator pd)
    {
        if (entity == null)
        {
            throw new IllegalArgumentException("Entity is null, can't check whether it's in persistence context");
        }
        Object primaryKey = PropertyAccessorHelper.getId(entity, entityMetadata);

        if (primaryKey == null)
        {
            throw new IllegalArgumentException("Primary key not set into entity");
        }
        String nodeId = ObjectGraphUtils.getNodeId(primaryKey, entity.getClass());
        return getNodeFromCache(nodeId, pd);
    }

    public synchronized void addNodeToCache(Node node)
    {
        // Make a deep copy of Node data and and set into node
        // Original data object is now detached from Node and is possibly
        // referred by user code
        Object nodeDataCopy = ObjectUtils.deepCopy(node.getData(), node.getPersistenceDelegator().getKunderaMetadata());
        node.setData(nodeDataCopy);

        /*
         * check if this node already exists in cache node mappings If yes,
         * update parents and children links Otherwise, just simply add the node
         * to cache node mappings
         */

        processNodeMapping(node);

        if (l2Cache != null)
        {
            l2Cache.put(node.getNodeId(), node.getData());
        }
    }

    public void processNodeMapping(Node node)
    {
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

            nodeMappings.put(node.getNodeId(), node);
            logCacheEvent("ADDED TO ", node.getNodeId());
        }
        else
        {
            logCacheEvent("ADDED TO ", node.getNodeId());
            nodeMappings.put(node.getNodeId(), node);
        }

        // If it's a head node, add this to the list of head nodes in
        // Persistence Cache
        if (node.isHeadNode())
        {
            node.getPersistenceCache().getMainCache().addHeadNode(node);
        }
    }

    public synchronized void removeNodeFromCache(Node node)
    {
        if (getHeadNodes().contains(node))
        {
            getHeadNodes().remove(node);
        }

        if (nodeMappings.get(node.getNodeId()) != null)
        {
            nodeMappings.remove(node.getNodeId());
        }

        evictFroml2Cache(node);
        logCacheEvent("REMOVED FROM ", node.getNodeId());
        node = null; // Eligible for GC
    }

    public void addGraphToCache(ObjectGraph graph, PersistenceCache persistenceCache)
    {
        // Add each node in the graph to cache
        for (String key : graph.getNodeMapping().keySet())
        {
            Node thisNode = graph.getNodeMapping().get(key);
            addNodeToCache(thisNode);

            // Remove all those head nodes in persistence cache, that are there
            // in Graph as a non-head node
            if (!thisNode.isHeadNode() && persistenceCache.getMainCache().getHeadNodes().contains(thisNode))
            {
                persistenceCache.getMainCache().getHeadNodes().remove(thisNode);
            }
        }
        // Add head Node to list of head nodes
        addHeadNode(graph.getHeadNode());
    }

    private void logCacheEvent(String eventType, String nodeId)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Node: " + nodeId + ":: " + eventType + " Persistence Context");
        }
    }

    /**
     * @param nodeMappings
     *            the nodeMappings to set
     */
    public void setNodeMappings(Map<String, Node> nodeMappings)
    {
        this.nodeMappings = nodeMappings;
    }

    public synchronized void addHeadNode(Node headNode)
    {
        headNodes.add(headNode);
    }

    public int size()
    {
        return nodeMappings.size();
    }

    public Collection<Node> getAllNodes()
    {
        return nodeMappings.values();
    }

    /**
     * 
     */
    public void clear()
    {
        if (this.nodeMappings != null)
        {
            this.nodeMappings.clear();
        }

        if (this.headNodes != null)
        {
            this.headNodes.clear();
        }

        if (this.l2Cache != null)
        {
            l2Cache.evictAll();
        }
    }

    /**
     * @return the headNodes
     */
    public Set<Node> getHeadNodes()
    {
        return Collections.synchronizedSet(headNodes);
    }

    private Node lookupL2Cache(String nodeId, PersistenceDelegator pd)
    {
        Node node = null;
        if (l2Cache != null)
        {
            Object entity = l2Cache.get(nodeId);
            if (entity != null)
            {
                node = new Node(nodeId, entity.getClass(), new ManagedState(), this.persistenceCache,
                        nodeId.substring(nodeId.indexOf("$") + 1), pd);
                node.setData(entity);
            }
        }

        return node;
    }

    private void evictFroml2Cache(Node node)
    {
        if (l2Cache != null)
        {
            this.l2Cache.evict(node.getDataClass(), node.getNodeId());
        }
    }
}
