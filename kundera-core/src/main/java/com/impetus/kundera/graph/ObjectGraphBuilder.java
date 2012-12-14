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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.collection.spi.PersistentCollection;

import com.impetus.kundera.Constants;
import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.DeepEquals;

/**
 * Responsible for generating {@link ObjectGraph} of nodes from a given entity
 * 
 * @author amresh.singh
 */
public class ObjectGraphBuilder
{
    private PersistenceCache persistenceCache;

    public ObjectGraphBuilder(PersistenceCache pcCache)
    {
        this.persistenceCache = pcCache;
    }

    public ObjectGraph getObjectGraph(Object entity, NodeState initialNodeState)
    {
        // Initialize object graph
        ObjectGraph objectGraph = new ObjectGraph();
//        this.persistenceCache = persistenceCache;

        // Recursively build object graph and get head node.
        Node headNode = getNode(entity, objectGraph, initialNodeState);

        // Set head node into object graph
        if (headNode != null)
        {
            objectGraph.setHeadNode(headNode);
        }
        return objectGraph;
    }

    public static String getNodeId(Object pk, Object nodeData)
    {
        StringBuffer strBuffer = new StringBuffer(nodeData.getClass().getName());
        strBuffer.append(Constants.NODE_ID_SEPARATOR);
        strBuffer.append(pk);
        return strBuffer.toString();
    }

    public static String getNodeId(Object pk, Class<?> objectClass)
    {

        StringBuffer strBuffer = new StringBuffer(objectClass.getName());
        strBuffer.append(Constants.NODE_ID_SEPARATOR);
        strBuffer.append(pk);
        return strBuffer.toString();

        // return objectClass.getName() + Constants.NODE_ID_SEPARATOR +
        // pk.toString();
    }

    /**
     * Constructs and returns {@link Node} representation for a given entity
     * object. Output is fully constructed graph with relationships embedded.
     * Each node is put into <code>graph</code> once it is constructed.
     * 
     * @param entity
     * @return
     */
    private Node getNode(Object entity, ObjectGraph graph, NodeState initialNodeState)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        if (entityMetadata == null)
        {
            return null;
        }
        Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
        String nodeId = ObjectGraphUtils.getNodeId(id, entity);

        // If this node is already there in graph (may happen for bidirectional
        // relationship, do nothing and return null)
        Node node = graph.getNode(nodeId);
        if (node != null)
        {
            if (node.isGraphCompleted())
            {
                return node;
            }
            else
            {
                return null;
            }
        }
        /*
         * Node node = graph.getNode(nodeId); if(node != null) { return node; }
         */

        // Construct this Node first, if one not already there in Persistence
        // Cache
        node = null;
        Node nodeInPersistenceCache = persistenceCache.getMainCache().getNodeFromCache(nodeId);

        // Make a deep copy of entity data
//        Object nodeDataCopy = ObjectUtils.deepCopy(entity);

        if (nodeInPersistenceCache == null)
        {
            node = new Node(nodeId, /*nodeDataCopy*/entity, initialNodeState, persistenceCache, id);

        }
        else
        {
            node = nodeInPersistenceCache;

            // Determine whether this node is dirty based on comparison between
            // Node data and entity data
            // If dirty, set the entity data into node and mark it as dirty
            if (!DeepEquals.deepEquals(node.getData(), entity))
            {
                node.setData(/*nodeDataCopy*/entity);
                node.setDirty(true);
            }
            else
            {
                node.setDirty(false);
            }

            // If node is NOT in managed state, its data needs to be
            // replaced with the one provided in entity object
            /*
             * if(!
             * node.getCurrentNodeState().getClass().equals(ManagedState.class))
             * { node.setData(nodeDataCopy); node.setDirty(true); }
             */
        }

        // Put this node into object graph
        graph.addNode(nodeId, node);

        // Iterate over relations and construct children nodes
        for (Relation relation : entityMetadata.getRelations())
        {

            // Child Object set in this entity
            Object childObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());

            if (childObject != null)
            {
                // This child object could be either an entity(1-1 or M-1) or a
                // collection of entities(1-M or M-M)
                if (Collection.class.isAssignableFrom(childObject.getClass()))
                {
                    // For each entity in the collection, construct a child node
                    // and add to graph
                    Collection childrenObjects = (Collection) childObject;

                    if(childrenObjects != null && !(childrenObjects instanceof PersistentCollection))
                    
                    for (Object childObj : childrenObjects)
                    {
                        if (childObj != null)
                        {
                            addChildNodesToGraph(graph, node, relation, childObj, initialNodeState);
                        }
                    }

                }
                else
                {
                    // Construct child node and add to graph
                    addChildNodesToGraph(graph, node, relation, childObject, initialNodeState);
                }
            }

        }

        // Means compelte graph is build.
        node.setGraphCompleted(true);
        return node;
    }

    /**
     * @param graph
     * @param node
     * @param relation
     * @param childObject
     */
    private void addChildNodesToGraph(ObjectGraph graph, Node node, Relation relation, Object childObject,
            NodeState initialNodeState)
    {
        // Construct child node for this child object via recursive call
        Node childNode = getNode(childObject, graph, initialNodeState);

        if (childNode != null)
        {
            // Construct Node Link for this relationship
            NodeLink nodeLink = new NodeLink(node.getNodeId(), childNode.getNodeId());
            nodeLink.setMultiplicity(relation.getType());

            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getDataClass());
            nodeLink.setLinkProperties(getLinkProperties(metadata, relation));

            // Add Parent node to this child
            childNode.addParentNode(nodeLink, node);

            // Add child node to this node
            node.addChildNode(nodeLink, childNode);
        }
    }

    /**
     * 
     * @param metadata
     *            Entity metadata of the parent node
     * @param relation
     * @return
     */
    private Map<LinkProperty, Object> getLinkProperties(EntityMetadata metadata, Relation relation)
    {
        Map<LinkProperty, Object> linkProperties = new HashMap<NodeLink.LinkProperty, Object>();

        linkProperties.put(LinkProperty.LINK_NAME, MetadataUtils.getMappedName(metadata, relation));
        linkProperties.put(LinkProperty.IS_SHARED_BY_PRIMARY_KEY, relation.isJoinedByPrimaryKey());
        linkProperties.put(LinkProperty.IS_BIDIRECTIONAL, !relation.isUnary());
        linkProperties.put(LinkProperty.IS_RELATED_VIA_JOIN_TABLE, relation.isRelatedViaJoinTable());
        linkProperties.put(LinkProperty.PROPERTY, relation.getProperty());
        // linkProperties.put(LinkProperty.BIDIRECTIONAL_PROPERTY, relation.ge);
        linkProperties.put(LinkProperty.CASCADE, relation.getCascades());

        if (relation.isRelatedViaJoinTable())
        {
            linkProperties.put(LinkProperty.JOIN_TABLE_METADATA, relation.getJoinTableMetadata());
        }

        // TODO: Add more link properties as required

        return linkProperties;
    }
}
