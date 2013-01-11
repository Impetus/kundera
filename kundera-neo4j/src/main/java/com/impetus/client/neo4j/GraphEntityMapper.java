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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.MapKeyJoinColumn;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute.CollectionType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.UniqueFactory;

import com.impetus.client.neo4j.index.AutoIndexing;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Responsible for converting Neo4J graph (nodes+relationships) into JPA entities and vice versa 
 * @author amresh.singh
 */
public class GraphEntityMapper
{
    
    /** The log. */
    private static Log log = LogFactory.getLog(GraphEntityMapper.class);

    public Node fromEntity(Object entity, List<RelationHolder> relations, GraphDatabaseService graphDb, EntityMetadata m)
    {
        AutoIndexing autoIndexing = new AutoIndexing();
        
        //Construct top level node first, making sure unique ID in the index        
        Node node = getOrCreateNodeWithUniqueFactory(entity, m, graphDb);
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        
        //Iterate over entity attributes
        Set<Attribute> attributes = entityType.getAttributes();      
        for(Attribute attribute : attributes)
        {       
            Field field = (Field) attribute.getJavaMember();
            
            //Set Node level properties
            if(! attribute.isCollection() && ! attribute.isAssociation())
            {
                
                String columnName = ((AbstractAttribute)attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);                
                node.setProperty(columnName, value);                
            }
            
            //If a property refers to other nodes through Map, Construct those nodes and add 
            //through relationship
            else if(attribute.isCollection() && attribute.isAssociation())
            {
                MapAttribute mapAttribute = entityType.getDeclaredMap(field.getName());                
                if(mapAttribute != null && mapAttribute.getCollectionType().equals(CollectionType.MAP))
                {
                    Field mapField = (Field)mapAttribute.getJavaMember();                    
                    
                    MapKeyJoinColumn mapKeyJoinColumnAnn = mapField.getAnnotation(MapKeyJoinColumn.class);
                    if(mapKeyJoinColumnAnn != null)
                    {
                        Map mapObject = (Map)PropertyAccessorHelper.getObject(entity, mapField);
                        Class<?> valueClass = mapAttribute.getBindableJavaType();    //Class for adjoining node object
                        Class<?> keyClass = mapAttribute.getKeyJavaType();           //Class for relationship object
                        
                        DynamicRelationshipType relType  = DynamicRelationshipType.withName(mapKeyJoinColumnAnn.name());
                        
                        EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(valueClass);
                        
                        for(Object key : mapObject.keySet())
                        {
                            
                            //If child entities are meant for Neo4J, Create "Real" nodes.                            
                            if(isEntityForNeo4J(childMetadata))
                            {
                                //Construct Adjoining Node
                                Object value = mapObject.get(key);                            
                                Node childNode = fromEntity(value, null, graphDb, childMetadata);
                                
                                //Connect adjoining node through relationships                            
                                Relationship relationship = node.createRelationshipTo(childNode, relType);                            
                                
                                //Set relations's own attributes into it
                                for(Field f : keyClass.getDeclaredFields())
                                {
                                    if(! f.getType().equals(valueClass) && !f.getType().equals(m.getEntityClazz()))
                                    {
                                        String relPropertyName = f.getAnnotation(Column.class) != null 
                                            ? f.getAnnotation(Column.class).name() : f.getName();                                    
                                        relationship.setProperty(relPropertyName, PropertyAccessorHelper.getObject(key, f));
                                    }
                                } 
                                
                                //TODO: If relationship auto-indexing is disabled, manually index this relationship
                                if(! autoIndexing.isRelationshipAutoIndexingEnabled(graphDb))
                                {
                                    
                                }
                                
                            }
                            
                            //Otherwise, create "Proxy" nodes
                            else
                            {
                                //TODO: Write code for create proxy nodes, as part of Polyglot implementation
                            }             
                            
                        }
                    }                
                    
                }
            }
        }
        
        //TODO: If node auto-indexing is disabled, manually index this node
        if(! autoIndexing.isNodeAutoIndexingEnabled(graphDb))
        {
            
        }
        
        return node;
    }

    public Object toEntity(Node node, List<String> relationNames, EntityMetadata m)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        
        //Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getAttributes();      
        
        Object entity = null;   
        
        try
        {
            entity = m.getEntityClazz().newInstance();
            
            for(Attribute attribute : attributes)
            {       
                Field field = (Field) attribute.getJavaMember();  
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                
                //Set Entity level properties
                if(! attribute.isCollection() && ! attribute.isAssociation())
                {
                    PropertyAccessorHelper.set(entity, field, node.getProperty(columnName));               
                    
                }
                
                //TODO: Set Relations
            
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
    
    private Node getOrCreateNodeWithUniqueFactory(Object entity, EntityMetadata m, GraphDatabaseService graphDb)
    {
        Object id = PropertyAccessorHelper.getObject(entity, (Field) m.getIdAttribute().getJavaMember());
        final String idFieldName = m.getIdAttribute().getName();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, m.getIndexName())
        {
            @Override
            protected void initialize(Node created, Map<String, Object> properties)
            {
                created.setProperty(idFieldName, properties.get(idFieldName));
            }
        }; 
        
        
        
        return factory.getOrCreate(idFieldName, id );
    }
    
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
    
    private boolean isEntityForNeo4J(EntityMetadata entityMetadata)
    {
        String persistenceUnit = entityMetadata.getPersistenceUnit();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
        String clientFactory = puMetadata.getProperty(PersistenceProperties.KUNDERA_CLIENT_FACTORY);
        if(clientFactory.indexOf("com.impetus.client.neo4j") > 0)
        {
            return true;
        }
        return false;
    }
    

}
