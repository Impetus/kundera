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
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.persistence.CascadeType;
import javax.persistence.PersistenceException;
import javax.swing.text.StyledEditorKit.ForegroundAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.Relation;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
			
			//Populate primary key column
			PropertyAccessorHelper.set(entity, m.getIdProperty(), document.get(m.getIdProperty().getName()));
			
			
			//Populate @Embedded objects and collections
			List<SuperColumn> superColumns = m.getSuperColumnsAsList();
			for(SuperColumn superColumn : superColumns) {
				Field superColumnField = superColumn.getField();
				Object embeddedDocumentObject = document.get(superColumnField.getName());	//Can be a BasicDBObject or a list of it.
				if(embeddedDocumentObject != null) {
					if(embeddedDocumentObject instanceof BasicDBList) {
						Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(superColumnField);
						Collection embeddedCollection = DocumentObjectMapper.getCollectionFromDocumentList((BasicDBList)embeddedDocumentObject, superColumnField.getType(), embeddedObjectClass, superColumn.getColumns());
						PropertyAccessorHelper.set(entity, superColumnField, embeddedCollection);						
					} else if(embeddedDocumentObject instanceof BasicDBObject){
						 Object embeddedObject = DocumentObjectMapper.getObjectFromDocument((BasicDBObject)embeddedDocumentObject,
								 superColumn.getField().getType(), superColumn.getColumns());
						 PropertyAccessorHelper.set(entity, superColumnField, embeddedObject);			
						
					} else {
						throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz " +
								"it wasn't stored as BasicDBObject, possible problem in format.");
					}
				}
			}
			
			//Check relations and fetch data from foreign keys list column
			List<Relation> relations = m.getRelations();	        
	        for(Relation relation : relations) {
	        	Class<?> embeddedEntityClass = relation.getTargetEntity(); 	//Embedded entity class
				Field embeddedPropertyField = relation.getProperty();		//Mapped to this property			
													
				
				EntityMetadata relMetadata = em.getMetadataManager().getEntityMetadata(embeddedEntityClass);			
				BasicDBList relList = (BasicDBList) document.get(embeddedPropertyField.getName());;		//List foreign keys	
				
				if(relList != null) {
					if(relation.isUnary()) {
						String foreignKey = (String)relList.get(0);
						
						Object embeddedEntity = null;
						try {
							embeddedEntity = em.getClient().loadColumns(em, embeddedEntityClass, relMetadata.getSchema(), 
									relMetadata.getTableName(), foreignKey, relMetadata);
						} catch (Exception e) {
							throw new PersistenceException("Error while fetching relationship entity " + relMetadata.getTableName()
									+ " from " + m.getTableName());
						}
						PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedEntity);	
					} else if(relation.isCollection()) {
						List<String> foreignKeys = new ArrayList<String>();
						for(Object o : relList) {
							foreignKeys.add((String) o);
						}					
						
						Collection embeddedEntityList = null;				//Collection of embedded entities			
						
						try {
							embeddedEntityList = em.getClient().loadColumns(em, embeddedEntityClass, relMetadata.getSchema(),
									relMetadata.getTableName(), relMetadata, foreignKeys.toArray(new String[0]));
						} catch (Exception e) {
							throw new PersistenceException("Error while fetching relationship entity collection  " + relMetadata.getTableName()
									+ " from " + m.getTableName());
						}
						
						Collection<Object> embeddedObjects = null;				//Collection of embedded entities
						if (relation.getPropertyType().equals(Set.class)) {
							embeddedObjects = new HashSet<Object>();
						} else if (relation.getPropertyType().equals(List.class)) {
							embeddedObjects = new ArrayList<Object>();
						}		
						
						embeddedObjects.addAll(embeddedEntityList);
						PropertyAccessorHelper.set(entity, embeddedPropertyField, embeddedObjects);
						
					}
					
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
	
	public BasicDBObject getDocumentFromEntity(EntityManagerImpl em, EntityMetadata m, EnhancedEntity e) throws PropertyAccessException {		
		List<Column> columns = m.getColumnsAsList();
		BasicDBObject dbObj = new BasicDBObject();	
		
		//Populate columns
		for(Column column : columns) {
			try {				
				extractEntityField(e.getEntity(), dbObj, column);						
			} catch (PropertyAccessException e1) {				
				log.error("Can't access property " + column.getField().getName());
			}
		}		
		
		//Populate @Embedded objects and collections
		List<SuperColumn> superColumns = m.getSuperColumnsAsList();
		for(SuperColumn superColumn : superColumns) {
			Field superColumnField = superColumn.getField();
			Object embeddedObject = PropertyAccessorHelper.getObject(e.getEntity(), superColumnField);
			if(embeddedObject != null) {
				if(embeddedObject instanceof Collection) {
					Collection embeddedCollection = (Collection) embeddedObject;					
					dbObj.put(superColumnField.getName(), DocumentObjectMapper.getDocumentListFromCollection(embeddedCollection, superColumn.getColumns()));						
				} else {					
					dbObj.put(superColumnField.getName(), DocumentObjectMapper.getDocumentFromObject(embeddedObject, superColumn.getColumns()));
				}
			}
		}
		
		//Check foreign keys and set as list column on document object
		Map<String, Set<String>> foreignKeyMap = e.getForeignKeysMap();
		Set foreignKeyNameSet = foreignKeyMap.keySet();
		for(Object foreignKeyName : foreignKeyNameSet) {
			Set valueSet = foreignKeyMap.get(foreignKeyName);
			BasicDBList foreignKeyValueList = new BasicDBList();
			for(Object o : valueSet) {
				foreignKeyValueList.add(o);
			}			
			dbObj.put((String)foreignKeyName, foreignKeyValueList);
		}
		
		return dbObj;
	}	
	

	/**
	 * @param entity
	 * @param dbObj
	 * @param column
	 * @throws PropertyAccessException
	 */
	private void extractEntityField(Object entity, BasicDBObject dbObj, Column column) throws PropertyAccessException {
		//A column field may be a collection(not defined as 1-to-M relationship)
		if(column.getField().getType().equals(List.class) || column.getField().getType().equals(Set.class)) {
			Collection collection = (Collection)PropertyAccessorHelper.getObject(entity, column.getField());								
			BasicDBList basicDBList = new BasicDBList();
			for(Object o : collection) {
				basicDBList.add(o);
			}			
			dbObj.put(column.getName(), basicDBList);
		} else {
			dbObj.put(column.getName(), PropertyAccessorHelper.getString(entity, column.getField()));
		}
	}	
	
	/**
	 * Returns column name from the filter property which is in the form dbName.columnName
	 * @param filterProperty
	 * @return
	 */
	public String getColumnName(String filterProperty) {
		StringTokenizer st = new StringTokenizer(filterProperty, ".");
		String columnName = "";
		while(st.hasMoreTokens()) {
			columnName = st.nextToken();
		}
		return columnName;	
	}
	
	/**
	 * Creates MongoDB Query object from filterClauseQueue
	 * @param filterClauseQueue
	 * @return
	 */
	public BasicDBObject createMongoDBQuery(Queue filterClauseQueue) {
		BasicDBObject query = new BasicDBObject();        
        
		for (Object object : filterClauseQueue) {
            if (object instanceof FilterClause) {
            	FilterClause filter = (FilterClause) object;
            	String property = new MongoDBDataHandler().getColumnName(filter.getProperty());
            	String condition = filter.getCondition();
            	String value = filter.getValue();            	
            	
            	if(condition.equals("=")) {
            		query.append(property, value);
            	} else if(condition.equalsIgnoreCase("like")) {
            		query.append(property,  Pattern.compile(value));
            	}  
            	//TODO: Add support for other operators like >, <, >=, <=, order by asc/ desc, limit, skip, count etc
            }
		}
		return query;
	}
}