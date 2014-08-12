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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.GeneratedValue;

import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.lifecycle.states.NodeState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.IdGenerator;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.PersistenceValidator;
import com.impetus.kundera.persistence.context.PersistenceCache;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.validation.rules.NullOrInvalidEntityRule;
import com.impetus.kundera.validation.rules.PrimaryKeyNullCheck;

/**
 * @author vivek.mishra
 * 
 *         Responsible for:
 * 
 *         1. Do pre-checks a) validation entity b) set id c) check for shared
 *         via primary key { on parse relation } d) dirty check { on associated
 *         and self entity }
 * 
 *         2. Parse through relations and relational objects.
 * 
 *         3. Recursively process transitive relations
 */
public final class GraphGenerator
{

    private GraphBuilder builder = new GraphBuilder();

    Set<Node> traversedNodes = new HashSet<Node>();

    private PersistenceValidator validator = new PersistenceValidator();

    /**
     * Generate entity graph and returns after assigning headnode. n
     * 
     * @param entity
     *            entity.
     * @param delegator
     *            delegator
     * @param pc
     *            persistence cache
     * @return object graph.
     */
    public <E> ObjectGraph generateGraph(E entity, PersistenceDelegator delegator)
    {
        this.builder.assign(this);
        Node node = generate(entity, delegator, delegator.getPersistenceCache(), null);
        this.builder.assignHeadNode(node);
        return this.builder.getGraph();
    }

    // TODO: Till the time issue populating all objects via cache is not yet
    // solved.
    // keep this method alive!

    /**
     * Generate entity graph and returns after assigning headnode. n
     * 
     * @param entity
     *            entity.
     * @param delegator
     *            delegator
     * @param pc
     *            persistence cache
     * @return object graph.
     */
    public <E> ObjectGraph generateGraph(E entity, PersistenceDelegator delegator, NodeState state)
    {

        this.builder.assign(this);
        Node node = generate(entity, delegator, delegator.getPersistenceCache(), state);
        this.builder.assignHeadNode(node);
        return this.builder.getGraph();
    }

    /**
     * Generate graph for head node. <li>traverse through it's relational
     * entities recursively
     * 
     * @param entity
     *            entity object
     * @param delegator
     *            delegator object
     * @param pc
     *            persistence cache
     * @return head node
     */
    <E> Node generate(E entity, PersistenceDelegator delegator, PersistenceCache pc, NodeState state)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                entity.getClass());

        if (!new NullOrInvalidEntityRule<E>(entityMetadata).validate(entity))
        {
            Object entityId = onPreChecks(entity, delegator);

            // TODO::check for composite key.
            Node node = builder.buildNode(entity, delegator, entityId, state);

            node = traverseNode(entity, delegator, pc, entityId, node);

            return node;
        }

        return null;
    }

    private <E> Node traverseNode(E entity, PersistenceDelegator delegator, PersistenceCache pc, Object entityId,
            Node node)
    {
        if (node != null && !traversedNodes.contains(node))
        {
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                    entity.getClass());

            // parse relations

            for (Relation relation : entityMetadata.getRelations())
            {
                if (relation != null)
                {
                    Object childObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());

                    // if child object is valid and not a proxy
                    if (childObject != null && !ProxyHelper.isProxyOrCollection(childObject))
                    {
                        // check for join by primary key, collection and map
                        EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(
                                delegator.getKunderaMetadata(),
                                PropertyAccessorHelper.getGenericClass(relation.getProperty()));

                        // set id,in case joined by primary key.
                        childObject = onIfSharedByPK(relation, childObject, childMetadata, entityId);

                        node = builder.getRelationBuilder(childObject, relation, node)
                                .assignResources(delegator, pc, childMetadata).build().getNode();
                    }
                }
            }

            // node.setGraphCompleted(true);
            traversedNodes.add(node);
        }
        return node;
    }

    /**
     * Check and set if relation is set via primary key.
     * 
     * @param relation
     *            relation
     * @param childObject
     *            target entity
     * @param childMetadata
     *            target entity metadata
     * @param entityId
     *            entity id
     * @return target entity.
     */
    private Object onIfSharedByPK(Relation relation, Object childObject, EntityMetadata childMetadata, Object entityId)
    {
        if (relation.isJoinedByPrimaryKey())
        {
            PropertyAccessorHelper.setId(childObject, childMetadata, entityId);
        }

        return childObject;
    }

    /**
     * On pre checks before generating graph. performed checks: <li>Check if
     * entity is valid.</li> <li>generated and set id in case
     * {@link GeneratedValue} is present and not set.</li> <li>Check if primary
     * key is not null.</li>
     * 
     * 
     * @param entity
     *            entity
     * @param client
     *            client
     * @return entity id
     */
    private <E> Object onPreChecks(E entity, PersistenceDelegator delegator)
    {
        // pre validation.
        // check if entity is Null or with Invalid meta data!
        Object id = null;

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                entity.getClass());
        id = PropertyAccessorHelper.getId(entity, entityMetadata);

        // set id, in case of auto generation and still not set.

        if (ObjectGraphUtils.onAutoGenerateId((Field) entityMetadata.getIdAttribute().getJavaMember(), id))
        {
            id = new IdGenerator().generateAndSetId(entity, entityMetadata, delegator, delegator.getKunderaMetadata());
        }

        // check if id is set or not.
        new PrimaryKeyNullCheck<Object>().validate(id);

        validator.validate(entity, delegator.getKunderaMetadata());

        // }

        return id;
    }

    /**
     * On building child node
     * 
     * @param childObject
     *            child object
     * @param childMetadata
     *            child metadata
     * @param delegator
     *            persistence delegator
     * @param pc
     *            persistence cache
     * @param node
     *            node
     * @param relation
     *            entity relation
     */
    void onBuildChildNode(Object childObject, EntityMetadata childMetadata, PersistenceDelegator delegator,
            PersistenceCache pc, Node node, Relation relation)
    {
        Node childNode = generate(childObject, delegator, pc, null);
        if (childNode != null)
        {
            assignNodeLinkProperty(node, relation, childNode);
        }
    }

    /**
     * On assigning node link properties
     * 
     * @param node
     *            node
     * @param relation
     *            relation
     * @param childNode
     *            child node
     */
    private void assignNodeLinkProperty(Node node, Relation relation, Node childNode)
    {
        // Construct Node Link for this relationship
        NodeLink nodeLink = new NodeLink(node.getNodeId(), childNode.getNodeId());
        setLink(node, relation, childNode, nodeLink);
    }

    /**
     * Set link property
     * 
     * @param node
     *            node
     * @param relation
     *            relation
     * @param childNode
     *            target node
     * @param nodeLink
     *            node link(bridge)
     */
    void setLink(Node node, Relation relation, Node childNode, NodeLink nodeLink)
    {
        nodeLink.setMultiplicity(relation.getType());

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(node.getPersistenceDelegator()
                .getKunderaMetadata(), node.getDataClass());
        nodeLink.setLinkProperties(getLinkProperties(metadata, relation, node.getPersistenceDelegator()
                .getKunderaMetadata()));

        // Add Parent node to this child
        childNode.addParentNode(nodeLink, node);

        // Add child node to this node
        node.addChildNode(nodeLink, childNode);
    }

    /**
     * 
     * @param metadata
     *            Entity metadata of the parent node
     * @param relation
     * @return
     */
    private Map<LinkProperty, Object> getLinkProperties(EntityMetadata metadata, Relation relation,
            final KunderaMetadata kunderaMetadata)
    {
        Map<LinkProperty, Object> linkProperties = new HashMap<NodeLink.LinkProperty, Object>();

        linkProperties.put(LinkProperty.LINK_NAME, MetadataUtils.getMappedName(metadata, relation, kunderaMetadata));
        linkProperties.put(LinkProperty.IS_SHARED_BY_PRIMARY_KEY, relation.isJoinedByPrimaryKey());
        linkProperties.put(LinkProperty.IS_BIDIRECTIONAL, !relation.isUnary());
        linkProperties.put(LinkProperty.IS_RELATED_VIA_JOIN_TABLE, relation.isRelatedViaJoinTable());
        linkProperties.put(LinkProperty.PROPERTY, relation.getProperty());
        linkProperties.put(LinkProperty.CASCADE, relation.getCascades());

        if (relation.isRelatedViaJoinTable())
        {
            linkProperties.put(LinkProperty.JOIN_TABLE_METADATA, relation.getJoinTableMetadata());
        }
        return linkProperties;
    }

}
