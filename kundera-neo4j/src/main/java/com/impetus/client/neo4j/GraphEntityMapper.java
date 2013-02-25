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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.UniqueFactory;

import com.impetus.client.neo4j.index.Neo4JIndexManager;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Responsible for converting Neo4J graph (nodes+relationships) into JPA entities and vice versa 
 * @author amresh.singh
 */
public final class GraphEntityMapper
{     
    private static final String COMPOSITE_KEY_SEPARATOR = "-";
    
    private static final String PROXY_NODE_TYPE_KEY = "$NODE_TYPE$";
    private static final String PROXY_NODE_VALUE = "$PROXY_NODE$";

    /** The log. */
    private static Log log = LogFactory.getLog(GraphEntityMapper.class);
    
    private Neo4JIndexManager indexer;
    
    public GraphEntityMapper(Neo4JIndexManager indexer)
    {
        this.indexer = indexer;
    }

    /**
     * Fetches(and converts) {@link Node} instance from Entity object
     * 
     */
    public Node fromEntity(Object entity, List<RelationHolder> relations, GraphDatabaseService graphDb, EntityMetadata m, boolean isUpdate)
    {        
        
        //Construct top level node first, making sure unique ID in the index
        Node node = null;
        if(! isUpdate)
        {
            node = getOrCreateNodeWithUniqueFactory(entity, m, graphDb);
        }
        else
        {
            Object key = PropertyAccessorHelper.getId(entity, m);
            node = searchNode(key, m, graphDb);
        }        
        
        if(node != null)
        {
            populateNodeProperties(entity, m, node);
        }         
        
        return node;
    }  
    
    public Node createProxyNode(Object sourceNodeId, Object targetNodeId, GraphDatabaseService graphDb, 
            EntityMetadata sourceEntityMetadata, EntityMetadata targetEntityMetadata)
    {
        
        String sourceNodeIdColumnName = ((AbstractAttribute) sourceEntityMetadata.getIdAttribute()).getJPAColumnName();
        String targetNodeIdColumnName = ((AbstractAttribute) targetEntityMetadata.getIdAttribute()).getJPAColumnName();
        
        Node node = graphDb.createNode();
        node.setProperty(PROXY_NODE_TYPE_KEY, PROXY_NODE_VALUE);
        node.setProperty(sourceNodeIdColumnName, sourceNodeId);
        node.setProperty(targetNodeIdColumnName, targetNodeId);        
        return node;
    }

    
    
    /**
     * 
     * Converts a {@link Node} instance to entity object
     */
    public Object toEntity(Node node, EntityMetadata m)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        
        //Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();     
        
        Object entity = null;   
        
        try
        {
            entity = m.getEntityClazz().newInstance();
            
            for(Attribute attribute : attributes)
            {       
                Field field = (Field) attribute.getJavaMember();  
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                
                //Set Entity level properties
                if(metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()) && m.getIdAttribute().getJavaType().equals(field.getType()))
                {
                    Object idValue = deserializeIdAttributeValue(m, (String)node.getProperty(columnName));
                    PropertyAccessorHelper.set(entity, field, idValue);
                }
                else if(! attribute.isCollection() && ! attribute.isAssociation())
                {
                    
                    PropertyAccessorHelper.set(entity, field, fromNeo4JObject(node.getProperty(columnName), field));         
                    
                }        
            }   
        }
        catch (InstantiationException e)
        {            
            log.error("Error while converting Neo4j object to entity. Details:" + e.getMessage());
            throw new EntityReaderException("Error while converting Neo4j object to entity");
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while converting Neo4j object to entity. Details:" + e.getMessage());
            throw new EntityReaderException("Error while converting Neo4j object to entity");
        } catch(NotFoundException e)
        {
            log.info(e.getMessage());
            return null;
        }
        
        
        return entity;
    }
    
    /**
     * 
     * Converts a {@link Relationship} object to corresponding entity object
     */
    public Object toEntity(Relationship relationship, EntityMetadata topLevelEntityMetadata, Relation relation)
    {
        EntityMetadata relationshipEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation.getMapKeyJoinClass());
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                relationshipEntityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(relationshipEntityMetadata.getEntityClazz());
        
        //Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();     
        
        Object entity = null;   
        
        try
        {
            entity = relationshipEntityMetadata.getEntityClazz().newInstance();
            
            for(Attribute attribute : attributes)
            {       
                Field field = (Field) attribute.getJavaMember();  
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                
                //Set Entity level properties
                if(metaModel.isEmbeddable(relationshipEntityMetadata.getIdAttribute().getBindableJavaType()) 
                        && relationshipEntityMetadata.getIdAttribute().getJavaType().equals(field.getType()))
                {
                    Object idValue = deserializeIdAttributeValue(relationshipEntityMetadata, (String) relationship.getProperty(columnName));
                    PropertyAccessorHelper.set(entity, field, idValue);
                }                
                else if(! attribute.isCollection() && ! attribute.isAssociation() 
                        && ! field.getType().equals(topLevelEntityMetadata.getEntityClazz())
                        && ! field.getType().equals(relation.getTargetEntity()))
                {
                    Object value = relationship.getProperty(columnName);
                    PropertyAccessorHelper.set(entity, field, fromNeo4JObject(value, field));  
                    
                }        
            }   
        }
        catch (InstantiationException e)
        {            
            log.error("Error while converting Neo4j object to entity. Details:" + e.getMessage());
            throw new EntityReaderException("Error while converting Neo4j object to entity");
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while converting Neo4j object to entity. Details:" + e.getMessage());
            throw new EntityReaderException("Error while converting Neo4j object to entity");
        }  
        
        
        return entity;
    }
    
    /**
     * Populates Node properties from Entity object
     * @param entity
     * @param m
     * @param node
     */
    public void populateNodeProperties(Object entity, EntityMetadata m, Node node)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        
        //Iterate over entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();     
        for(Attribute attribute : attributes)
        {       
            Field field = (Field) attribute.getJavaMember();
            
            //Set Node level properties
            if(! attribute.isCollection() && ! attribute.isAssociation() && !((SingularAttribute)attribute).isId())
            {                
                String columnName = ((AbstractAttribute)attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);                
                if(value != null)
                {
                    node.setProperty(columnName, toNeo4JProperty(value));
                }                                
            }           
        }
    }
    
    /**
     * Populates a {@link Relationship} with attributes of a given relationship entity object <code>relationshipObj</code>
     * @param entityMetadata
     * @param targetNodeMetadata
     * @param relationship
     * @param relationshipObj
     */
    public void populateRelationshipProperties(EntityMetadata entityMetadata, EntityMetadata targetNodeMetadata,
            Relationship relationship, Object relationshipObj)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());     
        EntityType entityType = metaModel.entity(relationshipObj.getClass());
        
        
        Set<Attribute> attributes = entityType.getSingularAttributes();     
        for(Attribute attribute : attributes)
        {       
            Field f = (Field) attribute.getJavaMember();
            if (!f.getType().equals(entityMetadata.getEntityClazz())
                    && !f.getType().equals(targetNodeMetadata.getEntityClazz()))
            {
                EntityMetadata relMetadata = KunderaMetadataManager.getEntityMetadata(relationshipObj.getClass());
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                if(metaModel.isEmbeddable(relMetadata.getIdAttribute().getBindableJavaType())
                        && ((SingularAttribute)attribute).isId())
                {
                    //Populate Embedded attribute value into relationship 
                    Object id = PropertyAccessorHelper.getId(relationshipObj, relMetadata);
                    Object serializedIdValue = serializeIdAttributeValue(relMetadata, id);
                    relationship.setProperty(columnName, serializedIdValue);
                    
                    //Populate attributes of embedded fields into relationship
                    Set<Attribute> embeddableAttributes = metaModel.embeddable(relMetadata.getIdAttribute().getBindableJavaType()).getSingularAttributes();
                    for(Attribute embeddableAttribute : embeddableAttributes)
                    {
                        String embeddedColumn = ((AbstractAttribute) embeddableAttribute).getJPAColumnName();
                        if(embeddedColumn == null) embeddedColumn = embeddableAttribute.getName();
                        Object value = PropertyAccessorHelper.getObject(id, (Field) embeddableAttribute.getJavaMember());
                        
                        if(value != null) relationship.setProperty(embeddedColumn, value);                         
                    }
                }
                else
                {
                    Object value = PropertyAccessorHelper.getObject(relationshipObj, f);
                    relationship.setProperty(columnName, toNeo4JProperty(value));
                }   
                        
            }           
        }
        
       
    } 
    
    /**
     * Creates a Map containing all properties (and their values) for a given entity
     * @param entity
     * @param m
     * @return
     */
    public Map<String, Object> createNodeProperties(Object entity, EntityMetadata m)
    {
        Map<String, Object> props = new HashMap<String, Object>();
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());      
        
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        
        //Iterate over entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();     
        for(Attribute attribute : attributes)
        {       
            Field field = (Field) attribute.getJavaMember();
            
            //Set Node level properties
            if(! attribute.isCollection() && ! attribute.isAssociation())
            {
                
                String columnName = ((AbstractAttribute)attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);                
                if(value != null)
                {
                    props.put(columnName, toNeo4JProperty(value));                    
                }                                
            }           
        }
        return props;        
    }
    
    /**
     * Creates a Map containing all properties (and their values) for a given relationship entity
     * @param entityMetadata
     * @param targetEntityMetadata
     * @param relationshipProperties
     * @param relationshipEntity
     */
    public Map<String, Object> createRelationshipProperties(EntityMetadata entityMetadata, EntityMetadata targetEntityMetadata,
            Object relationshipEntity)
    {
        Map<String, Object> relationshipProperties = new HashMap<String, Object>();
        
        for (Field f : relationshipEntity.getClass().getDeclaredFields())
        {
            if (!f.getType().equals(entityMetadata.getEntityClazz())
                    && !f.getType().equals(targetEntityMetadata.getEntityClazz()))
            {
                String relPropertyName = f.getAnnotation(Column.class) != null ? f.getAnnotation(
                        Column.class).name() : f.getName();
                Object value = PropertyAccessorHelper.getObject(relationshipEntity, f);                                            
                relationshipProperties.put(relPropertyName, toNeo4JProperty(value));                                           
            }
        }
        return relationshipProperties;
    }
    
    /**
     * 
     * Gets (if available) or creates a node for the given entity
     */
    private Node getOrCreateNodeWithUniqueFactory(final Object entity, final EntityMetadata m, GraphDatabaseService graphDb)
    {
        final Object id = PropertyAccessorHelper.getObject(entity, (Field) m.getIdAttribute().getJavaMember());       
        final String idColumnName = ((AbstractAttribute)m.getIdAttribute()).getJPAColumnName();   
       
         
        
        final MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());      
        
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, m.getIndexName())
        {            
            /** 
             * Initialize ID attribute
             */
            @Override
            protected void initialize(Node created, Map<String, Object> properties)
            {   
                //Set Embeddable ID attribute
                if(metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
                {                   
                    //Populate embedded field value in serialized format
                    String idUniqueValue = serializeIdAttributeValue(m, id);
                    created.setProperty(idColumnName, idUniqueValue);
                    
                    //Populated all other attributes of embedded into this node 
                    Set<Attribute> embeddableAttributes = metaModel.embeddable(m.getIdAttribute().getBindableJavaType()).getSingularAttributes();
                    for(Attribute attribute : embeddableAttributes)
                    {
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        if(columnName == null) columnName = attribute.getName();
                        Object value = PropertyAccessorHelper.getObject(id, (Field)attribute.getJavaMember());
                        
                        if(value != null) created.setProperty(columnName, value);                         
                    }                   
                }
                else 
                {                    
                    created.setProperty(idColumnName, properties.get(idColumnName));                    
                }
                                
            }
        };        
        
        if(metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {                      
            String idUniqueValue = serializeIdAttributeValue(m, id);
            return factory.getOrCreate(idColumnName, idUniqueValue);
        }
        else
        {
            return factory.getOrCreate(idColumnName, id );
        }
        
    }

    /**
     * @param m
     * @param id
     * @param metaModel
     * @return
     */
    private String serializeIdAttributeValue(final EntityMetadata m, Object id)
    {/*
        final MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit()); */ 
        Class<?> embeddableClass = m.getIdAttribute().getBindableJavaType();
        
        String idUniqueValue = "";
               
        for(Field embeddedField : embeddableClass.getDeclaredFields())
        {
            /*Column columnAnn = embeddedField.getAnnotation(Column.class);
            String columnName = columnAnn == null || columnAnn.name() == null ? embeddedField.getName() : columnAnn.name();*/         
            
            Object value = PropertyAccessorHelper.getObject(id, embeddedField);
            idUniqueValue = idUniqueValue + value + COMPOSITE_KEY_SEPARATOR;
        }
        
        if(idUniqueValue.endsWith(COMPOSITE_KEY_SEPARATOR)) idUniqueValue = idUniqueValue.substring(0, idUniqueValue.length() - 1);
        return idUniqueValue;
    }

    private Object deserializeIdAttributeValue(final EntityMetadata m, String idValue)
    {
        Class<?> embeddableClass = m.getIdAttribute().getBindableJavaType();
        Object embeddedObject = null;
        try
        {
            embeddedObject = embeddableClass.newInstance();
        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + embeddableClass + ". Did you define no argument constructor? Details:" + e.getMessage());
            return embeddedObject;
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while instantiating " + embeddableClass + ".? Details:" + e.getMessage());
            return embeddedObject;
        }

        String[] tokens = idValue.split(COMPOSITE_KEY_SEPARATOR);

        int count = 0;
        for (Field embeddedField : embeddableClass.getDeclaredFields())
        {
            if(count < tokens.length)
            {
                String value = tokens[count++];
                PropertyAccessorHelper.set(embeddedObject, embeddedField, value);
            }            
        }
        return embeddedObject;
    }
    
    /**
     * Gets (if available) or creates a relationship for the given entity
     */
    private Relationship getOrCreateRelationshipWithUniqueFactory(Object entity, EntityMetadata m, GraphDatabaseService graphDb)
    {
        Object id = PropertyAccessorHelper.getObject(entity, (Field) m.getIdAttribute().getJavaMember());
        final String idFieldName = m.getIdAttribute().getName();
        
        UniqueFactory<Relationship> factory = new UniqueFactory.UniqueRelationshipFactory(graphDb, m.getIndexName())
        {
            
            @Override
            protected Relationship create(Map<String, Object> paramMap)
            {
                return null;
            }
            
            @Override
            protected void initialize(Relationship relationship, Map<String,Object> properties) {
                relationship.setProperty(idFieldName, properties.get(idFieldName));
            }
        };
        
        return factory.getOrCreate(idFieldName, id);
    }
    
    /**
     * Converts a given field value to an object that is Neo4J compatible
     * 
     * @param source
     * @return
     */
    public Object toNeo4JProperty(Object source)
    {
        
        Class<?> sourceClass = source.getClass();
        if(source instanceof BigDecimal || source instanceof BigInteger)
        {
            return source.toString();
        }
        else if ((source instanceof Calendar) || (source instanceof GregorianCalendar))
        {            
            return PropertyAccessorHelper.fromSourceToTargetClass(String.class, Date.class, ((Calendar) source).getTime());
        }
        if (source instanceof Date)
        {            
            return PropertyAccessorHelper.fromSourceToTargetClass(String.class, sourceClass, source);
        }       

        return source;
    } 
    
    /**
     * Converts a property stored in Neo4J (nodes or relationship) to corresponding entity field value
     */
    public Object fromNeo4JObject(Object source, Field field)
    {    
        Class<?> targetClass = field.getType();
        
        if(targetClass.isAssignableFrom(BigDecimal.class) 
                || targetClass.isAssignableFrom(BigInteger.class)
                )
        {
            return PropertyAccessorHelper.fromSourceToTargetClass(field.getType(), source.getClass(), source);
        }
        else if(targetClass.isAssignableFrom(Calendar.class)
                || targetClass.isAssignableFrom(GregorianCalendar.class))
        {
            Date d = (Date)PropertyAccessorHelper.fromSourceToTargetClass(Date.class, source.getClass(), source);            
            Calendar cal =  Calendar.getInstance(); cal.setTime(d); return cal;
        }
        else if(targetClass.isAssignableFrom(Date.class))
        {
            return PropertyAccessorHelper.fromSourceToTargetClass(field.getType(), source.getClass(), source);
        }
        else
        {
            return source;
        }
        
    }
    
    /**
     * Searches a node from the database for a given key
     */
    public Node searchNode(Object key, EntityMetadata m, GraphDatabaseService graphDb)
    {
        Node node = null;  

        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        
        final MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());  
        if(metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            key = serializeIdAttributeValue(m, key);
        }        
        
        if (indexer.isNodeAutoIndexingEnabled(graphDb))
        {
            // Get the Node auto index
            ReadableIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            IndexHits<Node> nodesFound = autoNodeIndex.get(idColumnName, key);
            if (nodesFound == null || nodesFound.size() == 0 || ! nodesFound.hasNext())
            {
                return null;
            }            
            else
            {
                node = nodesFound.next();
            }
            nodesFound.close();
        }
        else
        {
            //Searching within manually created indexes
            Index<Node> nodeIndex = graphDb.index().forNodes(m.getIndexName());
            IndexHits<Node> hits = nodeIndex.get(idColumnName, key);
            if(hits == null || hits.size() == 0 || ! hits.hasNext())
            {
                return null;
            }
            else
            {
                node = hits.next();
            } 
            hits.close();
            
        }

        return node;
    }  

}
