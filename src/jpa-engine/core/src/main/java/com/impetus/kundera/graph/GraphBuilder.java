/**
 * Copyright 2013 Impetus Infotech.
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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.persistence.EmbeddedId;
import javax.persistence.MapKeyJoinColumn;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.lifecycle.states.TransientState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.utils.DeepEquals;

/**
 * Assign head node set relational node: 1. check for proxy 2. graph status of
 * node.
 * 
 * @author vivek.mishra
 * 
 */
public class GraphBuilder
{
    private ObjectGraph graph;

    private GraphGenerator generator;

    public GraphBuilder()
    {
        this.graph = new ObjectGraph();
    }

    /**
     * Assign generator reference.
     * 
     * @param generator
     *            graph generator.
     */
    public void assign(GraphGenerator generator)
    {
        this.generator = generator;
    }

    /**
     * On build node.
     * 
     * @param entity
     *            entity
     * @param pc
     *            persistence cache
     * @param entityId
     *            entity id
     * @return added node.
     */
    public final Node buildNode(Object entity, PersistenceDelegator pd, Object entityId, NodeState nodeState)
    {
        String nodeId = ObjectGraphUtils.getNodeId(entityId, entity.getClass());

        Node node = this.graph.getNode(nodeId);

        // If this node is already there in graph (may happen for bidirectional
        // relationship, do nothing and return null)
        // return node in case has already been traversed.

        if (node != null)
        {
            if (this.generator.traversedNodes.contains(node))
            {
                return node;
            }

            return null;
        }

        node = new NodeBuilder().assignState(nodeState).buildNode(entity, pd, entityId, nodeId).node;
        this.graph.addNode(node.getNodeId(), node);
        return node;

    }

    /**
     * Assign head node to graph
     * 
     * @param headNode
     * @return graph builder instance.
     */
    public GraphBuilder assignHeadNode(final Node headNode)
    {
        this.graph.setHeadNode(headNode);
        return this;
    }

    /**
     * Returns relation builder instance.
     * 
     * @param target
     *            relational entity
     * @param relation
     *            relation
     * @param source
     *            relation originator entity
     * @return relation builder instance.
     */
    RelationBuilder getRelationBuilder(Object target, Relation relation, Node source)
    {
        RelationBuilder relationBuilder = new RelationBuilder(target, relation, source);
        relationBuilder.assignGraphGenerator(this.generator);
        return relationBuilder;
    }

    /**
     * Returns completed graph.
     * 
     * @return object graph.
     */
    ObjectGraph getGraph()
    {
        return this.graph;
    }

    /**
     * Inner class {Relation builder}
     * 
     * @author vivek.mishra
     * 
     */
    class RelationBuilder
    {
        private GraphGenerator generator;

        private Object target;

        private Node source;

        private EntityMetadata metadata;

        private PersistenceDelegator pd;

        private PersistenceCache pc;

        private Relation relation;

        private RelationBuilder(Object target, Relation relation, Node source)
        {
            this.target = target;
            this.relation = relation;
            this.source = source;
        }

        /**
         * Assign graph generator
         * 
         * @param generator
         *            graph generator
         * 
         * @return
         */
        private RelationBuilder assignGraphGenerator(GraphGenerator generator)
        {
            this.generator = generator;
            return this;
        }

        /**
         * Assign relation builder resources
         * 
         * @param pd
         *            persistence delegator
         * @param pc
         *            persistence cache
         * @param metadata
         *            entity meta data
         * @return relation builder
         */
        RelationBuilder assignResources(final PersistenceDelegator pd, final PersistenceCache pc,
                final EntityMetadata metadata)
        {
            this.pc = pc;
            this.pd = pd;
            this.metadata = metadata;
            return this;
        }

        /**
         * Build relation
         * 
         * @return relation builder
         */
        RelationBuilder build()
        {
            if (!onNonUnaryRelation())
            {
                this.generator.onBuildChildNode(target, metadata, this.pd, pc, source, relation);
            }

            return this;
        }

        /**
         * parse and process non unary relations {e.g. 1-M and M-M}
         * 
         * @return true, if is a non unary relation and processed.
         */
        private boolean onNonUnaryRelation()
        {
            if (!relation.isUnary())
            {
                if (Collection.class.isAssignableFrom(target.getClass()))
                {
                    Collection childrenObjects = (Collection) target;

                    for (Object childObj : childrenObjects)
                    {
                        if (childObj != null)
                        {
                            this.generator.onBuildChildNode(childObj, metadata, this.pd, pc, source, relation);
                        }
                    }

                }
                else if (Map.class.isAssignableFrom(target.getClass()))
                {
                    Map childrenObjects = (Map) target;
                    if (childrenObjects != null && !ProxyHelper.isProxyCollection(childrenObjects))
                    {
                        for (Map.Entry entry : (Set<Map.Entry>) childrenObjects.entrySet())
                        {
                            Object relObject = entry.getKey();
                            Object entityObject = entry.getValue();
                            if (entityObject != null)
                            {
                                Node childNode = this.generator.generate(entityObject, pd, pc, null);
                                // in case node is already in cache.
                                if (childNode != null)
                                {
                                    if (StringUtils.isEmpty(relation.getMappedBy())
                                            && relation.getProperty().getAnnotation(MapKeyJoinColumn.class) != null)
                                    {
                                        NodeLink nodeLink = new NodeLink(source.getNodeId(), childNode.getNodeId());
                                        this.generator.setLink(source, relation, childNode, nodeLink);
                                        nodeLink.addLinkProperty(LinkProperty.LINK_VALUE, relObject);
                                    }
                                }
                            }
                        }
                    }
                }

                return true;
            }

            return false;

        }

        /**
         * Returns built node.
         * 
         * @return node
         */
        Node getNode()
        {
            return this.source;
        }
    }

    /**
     * Inner class { Node builder }
     * 
     * @author vivek.mishra
     * 
     */
    private class NodeBuilder
    {
        private Node node;

        private NodeState state;

        private NodeBuilder assignState(NodeState state)
        {
            this.state = state;
            return this;
        }

        /**
         * Build node. Check for: 1. Node state, whether in pc or not 2. Node
         * dirty check
         * 
         * @param entity
         *            originating entity
         * @param pc
         *            persistence cache.
         * @param entityId
         *            entity id
         * @param nodeId
         *            node id.
         * @return node builder instance.
         */
        private NodeBuilder buildNode(Object entity, PersistenceDelegator pd, Object entityId, String nodeId)
        {

            Node nodeInPersistenceCache = pd.getPersistenceCache().getMainCache().getNodeFromCache(nodeId, pd);

            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(pd.getKunderaMetadata(),
                    entity.getClass());

            // TODO: in case of composite key. it is a bit hack and should be
            // handled better.
            if (nodeInPersistenceCache == null)
            {
                this.state = state != null ? this.state : new TransientState();

                node = new Node(
                        nodeId,
                        entity,
                        ((Field) entityMetadata.getIdAttribute().getJavaMember()).isAnnotationPresent(EmbeddedId.class) ? new ManagedState()
                                : this.state, pd.getPersistenceCache(), entityId, pd);
            }
            else
            {
                node = nodeInPersistenceCache;
                node.setPersistenceCache(pd.getPersistenceCache());
                node.setTraversed(false);
            }

            // Determine whether this node is dirty based on comparison between
            // Node data and entity data
            // If dirty, set the entity data into node and mark it as dirty
            onDirtyCheck(entity, node);
            node.setData(entity);

            return this;
        }

        /**
         * Check for dirty.
         * 
         * @param entity
         *            entity
         * @param node
         *            node.
         */
        private void onDirtyCheck(Object entity, Node node)
        {
            if (!node.isInState(TransientState.class))
            {
                if (!DeepEquals.deepEquals(node.getData(), entity))
                {
                    node.setDirty(true);
                }
                else if (node.isProcessed())
                {
                    node.setDirty(false);
                }
            }
        }

    }
}