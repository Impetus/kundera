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

import com.impetus.kundera.graph.NodeLink.LinkProperty;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessorHelper;


/**
 * Responsible for generating {@link ObjectGraph} of nodes from a given entity
 * @author amresh.singh
 */
public class ObjectGraphBuilder
{
    public ObjectGraph getObjectGraph(Object entity) {        
        //Initialize object graph
        ObjectGraph objectGraph = new ObjectGraph();   
        
        //Recursively build object graph and get head node.
        Node headNode = getNode(entity, objectGraph);         
        
        //Set head node into object graph
        objectGraph.setHeadNode(headNode);
        
        return objectGraph;
    }


    /**
     * Constructs and returns {@link Node} representation for a given entity object.
     * Output is fully constructed graph with relationships embedded.
     * Each node is put into <code>graph</code> once it is constructed.
     * @param entity
     * @return
     */
    private Node getNode(Object entity, ObjectGraph graph)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        Object id = PropertyAccessorHelper.getId(entity, entityMetadata);        
        String nodeId = getNodeId(id, entity);
        
        //Construct this Node first
        Node node = new Node(nodeId, entity);
        
        //Iterate over relations and construct children nodes
        for(Relation relation : entityMetadata.getRelations()) {            
            
            //child Object set in this entity
            Object childObject =  PropertyAccessorHelper.getObject(entity, relation.getProperty());
            
            //This child object could be either an entity(1-1 or M-1) or a collection of entities(1-M or M-M)
            if(Collection.class.isAssignableFrom(childObject.getClass())) {
                //For each entity in the collection, construct a child node and add to graph
                Collection childrenObjects = (Collection) childObject;
                
                for(Object childObj : childrenObjects) {
                    addChildNodesToGraph(graph, node, relation, childObj);
                }                
                
                
            } else {
                //Construct child node and add to graph
                addChildNodesToGraph(graph, node, relation, childObject);
            }
            
        }        
        //Finally put this node into object graph
        graph.addNode(nodeId, node);
        return node;
    }


    /**
     * @param graph
     * @param node
     * @param relation
     * @param childObject
     */
    private void addChildNodesToGraph(ObjectGraph graph, Node node, Relation relation, Object childObject)
    {
        //Construct child node for this child object via recursive call
        Node childNode = getNode(childObject, graph);      
                        
        //Construct Node Link for this relationship
        NodeLink nodeLink = new NodeLink(node.getNodeId(), childNode.getNodeId());
        nodeLink.setMultiplicity(relation.getType());
        nodeLink.setLinkProperties(getLinkProperties(relation));
        
        //Add Parent node to this child
        childNode.addParentNode(nodeLink, node);
        
        //Add child node to this node
        node.addChildNode(nodeLink, childNode);
    }
    
    private Map<LinkProperty, Object> getLinkProperties(Relation relation) {
        Map<LinkProperty, Object> linkProperties = new HashMap<NodeLink.LinkProperty, Object>();
        
        linkProperties.put(LinkProperty.JOIN_COLUMN_NAME, relation.getJoinColumnName());
        linkProperties.put(LinkProperty.IS_SHARED_BY_PRIMARY_KEY, relation.isJoinedByPrimaryKey());
        linkProperties.put(LinkProperty.IS_BIDIRECTIONAL, !relation.isUnary());
        linkProperties.put(LinkProperty.IS_RELATED_VIA_JOIN_TABLE, relation.isRelatedViaJoinTable());
        linkProperties.put(LinkProperty.PROPERTY, relation.getProperty());
        //linkProperties.put(LinkProperty.BIDIRECTIONAL_PROPERTY, relation.ge);        
        
        //Add All link properties
        
        return linkProperties;      
    }
    
    
    
    public static String getNodeId(Object pk, Object nodeData) {
        return nodeData.getClass().getName() + "_" + (String)pk;
    }
    
}
