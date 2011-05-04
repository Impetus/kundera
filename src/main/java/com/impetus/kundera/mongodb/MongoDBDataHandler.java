/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.mongodb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Provides utility methods for handling data held in MongoDB
 * @author amresh.singh
 */
public class MongoDBDataHandler {
	private static Log log = LogFactory.getLog(MongoDBDataHandler.class);
	
	public Object getEntityFromDocument(EntityManagerImpl em, Class<?> entityClass, EntityMetadata m, DBObject document) {
		Object entity = null;
		try {
			entity = entityClass.newInstance();
			
			//Populate entity columns
			List<Column> columns = m.getColumnsAsList();
			for(Column column : columns) {
				PropertyAccessorHelper.set(entity, column.getField(), document.get(column.getName()));
			}
			
			//Populate embedded relationship object
	        List<Relation> relations = m.getRelations();
	        
	        for(Relation relation : relations) {
	        	Class<?> embeddedEntityClass = relation.getTargetEntity(); 	//Embedded entity class
				Field embeddedPropertyField = relation.getProperty();		//Mapped to this property					
				boolean optional = relation.isOptional();					// Is it optional? TODO: Where to use this?									
				
				EntityMetadata relMetadata = em.getMetadataManager().getEntityMetadata(embeddedEntityClass);
				relMetadata.addColumn(relMetadata.getIdColumn().getName(), relMetadata.getIdColumn());	//Add PK column
				
				Object embeddedObject = document.get(embeddedPropertyField.getName());
				
				
				if(relation.isUnary()) {			
					BasicDBObject relDBObj = (BasicDBObject) embeddedObject;
					Object embeddedEntity = new MongoDBDataHandler().getEntityFromDocument(em, embeddedEntityClass, relMetadata, relDBObj);
					PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedEntity);				
				} else if(relation.isCollection()) {
					BasicDBList relList = (BasicDBList) embeddedObject;		//List of embedded objects
					
					Collection<Object> embeddedEntities = null;				//Collection of embedded entities
					if (relation.getPropertyType().equals(Set.class)) {
						embeddedEntities = new HashSet<Object>();
					} else if (relation.getPropertyType().equals(List.class)) {
						embeddedEntities = new ArrayList<Object>();
					}				
					
					for(int i = 0; i < relList.size(); i++) {
						BasicDBObject relObj = (BasicDBObject)relList.get(i);					
						Object embeddedEntity = new MongoDBDataHandler().getEntityFromDocument(em, embeddedEntityClass, relMetadata, relObj);
						embeddedEntities.add(embeddedEntity);						
					}	
					
					PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedEntities);	
				}				
	        }
			
		} catch (InstantiationException e) {
			log.error("Error while instantiating " + entityClass + ". Details:" + e.getMessage());
			return entity;
		} catch (IllegalAccessException e) {
			log.error("Error while Getting entity from Document. Details:" + e.getMessage());
			return entity;
		} catch (PropertyAccessException e) {
			log.error("Error while Getting entity from Document. Details:" + e.getMessage());
			return entity;
		}
        return entity;
	}
	
	public BasicDBObject getDocumentFromEntity(EntityManagerImpl em, EntityMetadata m, Object entity) throws PropertyAccessException {		
		List<Column> columns = m.getColumnsAsList();
		BasicDBObject dbObj = new BasicDBObject();	
		
		//Populate columns
		for(Column column : columns) {
			try {
				dbObj.put(column.getName(), PropertyAccessorHelper.getString(entity, column.getField()));
			} catch (PropertyAccessException e1) {				
				log.error("Can't access property " + column.getField().getName());
			}
		}		
		
		//Populate Relationship fields
		List<Relation> relations = m.getRelations();
		for(Relation relation : relations) {
			// Cascade?
			if (!relation.getCascades().contains(CascadeType.ALL)
					&& !relation.getCascades().contains(CascadeType.PERSIST)) {
				continue;
			}			

			Class<?> embeddedEntityClass = relation.getTargetEntity(); //Target entity
			Field embeddedEntityField = relation.getProperty();	//Mapped to this property			
			boolean optional = relation.isOptional();	// Is it optional? TODO: Where to use this?			
			Object embeddedObject = PropertyAccessorHelper.getObject(entity, embeddedEntityField); // Value			
			
			EntityMetadata relMetadata = em.getMetadataManager().getEntityMetadata(embeddedEntityClass);
			relMetadata.addColumn(relMetadata.getIdColumn().getName(), relMetadata.getIdColumn());	//Add PK column
			
			if(relation.isUnary()) {				
				
				BasicDBObject relDBObj = getDocumentFromEntity(em, relMetadata, embeddedObject);
				dbObj.put(embeddedEntityField.getName(), relDBObj);
				
			} else if(relation.isCollection()) {
				Collection collection = (Collection) embeddedObject;
				BasicDBObject[] relDBObjects = new BasicDBObject[collection.size()];
				int count = 0;
				for(Object o : collection) {
					relDBObjects[count] = getDocumentFromEntity(em, relMetadata, o);	
					count++;
				}
				dbObj.put(embeddedEntityField.getName(), relDBObjects);
			}			
		}		
		return dbObj;
	}	


}