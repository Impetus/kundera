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
package com.impetus.client.neo4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;

import com.impetus.client.neo4j.query.Neo4JQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.KunderaTransactionException;
import com.impetus.kundera.persistence.TransactionBinder;
import com.impetus.kundera.persistence.TransactionResource;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Implementation of {@link Client} using Neo4J Native Java driver (see Embedded
 * Java driver at http://www.neo4j.org/develop/drivers)
 * 
 * @author amresh.singh
 */
public class Neo4JClient extends Neo4JClientBase implements Client<Neo4JQuery>, TransactionBinder
{

    private static Log log = LogFactory.getLog(Neo4JClient.class);

    /**
     * Reference to Neo4J client factory.
     */
    private Neo4JClientFactory factory;

    private EntityReader reader;

    private GraphEntityMapper mapper;
    
    private TransactionResource resource;

    Neo4JClient(final Neo4JClientFactory factory)
    {
        this.factory = factory;
        reader = new Neo4JEntityReader();
        mapper = new GraphEntityMapper();
    }

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        // All client properties currently are those that are specified in
        // neo4j.properties.
        // No custom property currently defined by Kundera, leaving empty
    }

    /**
     * Finds an entity from graph database
     */
    @Override
    public Object find(Class entityClass, Object key)
    {

        GraphDatabaseService graphDb = factory.getConnection();
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);

        Object entity = null;

        Node node = mapper.searchNode(key, m, graphDb);
        if (node != null)

        {
            entity = mapper.toEntity(node, m);

            // Populate all relationship entities that are in Neo4J
            for (Relation relation : m.getRelations())
            {
                Class<?> targetEntityClass = relation.getTargetEntity();
                EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(targetEntityClass);                
                Field property = relation.getProperty();       
                
                
                if (relation.getPropertyType().isAssignableFrom(Map.class)
                        && relation.getType().equals(ForeignKey.MANY_TO_MANY) 
                        && isEntityForNeo4J(targetEntityMetadata))
                {
                    
                    if(relation.getFetchType() != null && relation.getFetchType().equals(FetchType.LAZY))
                    {
                        continue;
                    }
                    
                    Map targetEntitiesMap = new HashMap();                   

                    for (Relationship relationship : node.getRelationships(Direction.OUTGOING,
                            DynamicRelationshipType.withName(relation.getJoinColumnName())))
                    {                        
                        Node endNode = relationship.getEndNode();
                        Object targetEntity = mapper.toEntity(endNode, targetEntityMetadata);
                        Object relationshipEntity = mapper.toEntity(relationship, m, relation);
                        
                        //Set references to Target and owning entity in relationship entity
                        Class<?> relationshipClass = relation.getMapKeyJoinClass();
                        for(Field f : relationshipClass.getDeclaredFields())
                        {
                            if(f.getType().equals(m.getEntityClazz()))
                            {
                                PropertyAccessorHelper.set(relationshipEntity, f, entity);
                            }
                            else if(f.getType().equals(targetEntityClass))
                            {
                                PropertyAccessorHelper.set(relationshipEntity, f, targetEntity);
                            }
                        }
                        targetEntitiesMap.put(relationshipEntity, targetEntity);                        
                    }
                    
                    PropertyAccessorHelper.set(entity, property, targetEntitiesMap);

                }
            }
        }

        return entity;
    }

    

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {

        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    public void close()
    {
    }

    /**
     * Deletes an entity from database
     */
    @Override
    public void delete(Object entity, Object key)
    {
        GraphDatabaseService graphDb = factory.getConnection();
        checkActiveTransaction(); 

        try
        {
            // Find Node for this particular entity
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entity.getClass());
            Node node = mapper.searchNode(key, m, graphDb);
            if (node == null)
            {
                if(log.isDebugEnabled()) log.debug("Entity to be deleted doesn't exist in graph. Doing nothing");
                return;
            }

            // Remove this particular node
            node.delete();

            // Remove all relationship edges attached to this node (otherwise an
            // exception is thrown)
            for (Relationship relationship : node.getRelationships())
            {
                relationship.delete();
            }            
        }
        catch (Exception e)
        {
            log.error("Error while removing entity. Details:" + e.getMessage());
        }      

    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        throw new PersistenceException("Operation not supported for Neo4J");
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        System.out.println(colName);
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<Neo4JQuery> getQueryImplementor()
    {
        return null;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        if(log.isDebugEnabled()) log.debug("Persisting " + entity);        
        
        //All Modifying Neo4J operations must be executed within a transaction
        checkActiveTransaction();     
        
        GraphDatabaseService graphDb = factory.getConnection();  

        try
        {

            // Top level node
            Node node = mapper.fromEntity(entity, rlHolders, graphDb, entityMetadata, isUpdate);

            if (rlHolders != null && !rlHolders.isEmpty())
            {
                for (RelationHolder rh : rlHolders)
                {
                    // Search Node (to be connected to ) in Neo4J graph
                    EntityMetadata targetNodeMetadata = KunderaMetadataManager.getEntityMetadata(rh.getRelationValue()
                            .getClass());
                    Object targetNodeKey = PropertyAccessorHelper.getId(rh.getRelationValue(), targetNodeMetadata);
                    Node targetNode = mapper.searchNode(targetNodeKey, targetNodeMetadata, graphDb);

                    if (targetNode != null)
                    {
                        // Join this node (source node) to target node via
                        // relationship
                        DynamicRelationshipType relType = DynamicRelationshipType.withName(rh.getRelationName());
                        Relationship relationship = node.createRelationshipTo(targetNode, relType);

                        // Populate relationship's own properties into it
                        Object relationshipObj = rh.getRelationVia();
                        if (relationshipObj != null)
                        {
                            for (Field f : relationshipObj.getClass().getDeclaredFields())
                            {
                                if (!f.getType().equals(entityMetadata.getEntityClazz())
                                        && !f.getType().equals(targetNodeMetadata.getEntityClazz()))
                                {
                                    String relPropertyName = f.getAnnotation(Column.class) != null ? f.getAnnotation(
                                            Column.class).name() : f.getName();
                                    Object value = PropertyAccessorHelper.getObject(relationshipObj, f);
                                    relationship.setProperty(relPropertyName, mapper.toNeo4JObject(value));
                                            
                                }
                            }
                            EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(relationshipObj.getClass());
                            
                            //After relationship creation, manually index it if desired
                            manuallyIndexRelationship(relationMetadata, graphDb, relationship);
                        }                     
                    }

                }
            }

            //After node creation, mManually index this node, if desired
            manuallyIndexNode(entityMetadata, graphDb, node);
        }
        catch (Exception e)
        {            
            log.error("Error while persisting entity " + entity + ". Details:" + e.getMessage());
            throw new PersistenceException(e);
        }
    }

    
    
    
    @Override
    public void bind(TransactionResource resource)
    {
        if (resource != null && resource instanceof Neo4JTransaction)
        {
            ((Neo4JTransaction)resource).setGraphDb(factory.getConnection());
            this.resource = resource;
        }
        else
        {
            throw new KunderaTransactionException("Invalid transaction resource provided:" + resource
                    + " Should have been an instance of :" + Neo4JTransaction.class);
        }
    }
    
    /**
     * Checks whether there is an active transaction within this client
     * All Modifying Neo4J operations must be executed within a transaction
     */
    private void checkActiveTransaction()
    {
        if(resource == null || ! resource.isActive())
        {
            throw new NotInTransactionException("All Modifying Neo4J operations must be executed within a transaction");
        }
    }

}
