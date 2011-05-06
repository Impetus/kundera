/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.ejb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Class EntityReachabilityResolver.
 * 
 * @author animesh.kumar
 */
public class EntityResolver {

	/** The Constant log. */
	private static final Log log = LogFactory.getLog(EntityResolver.class);

	/** The em. */
	private EntityManagerImpl em;

	/**
	 * Instantiates a new entity resolver.
	 * 
	 * @param em
	 *            the em
	 */
	public EntityResolver(EntityManagerImpl em) {
		this.em = em;
	}

	/**
	 * Resolve all reachable entities from entity
	 * 
	 * @param entity
	 *            the entity
	 * @param cascadeType
	 *            the cascade type
	 * @return the all reachable entities
	 */
	public List<EnhancedEntity> resolve (Object entity, CascadeType cascadeType, DBType dbType) {
		Map<String, EnhancedEntity> map = new HashMap<String, EnhancedEntity>();
		try {
			log.debug("Resolving reachable entities for cascade "
							+ cascadeType);		
			
			//For Document-based data-stores, entities need to be embedded			
			if(dbType.equals(DBType.MONGODB)) {
				resolveEmbeddedEntities(entity, cascadeType, map);
			} else {
				recursivelyResolveEntities(entity, cascadeType, map);
			}
			
			
			
		} catch (PropertyAccessException e) {
			throw new PersistenceException(e.getMessage());
		}

		if (log.isDebugEnabled()) {
			for (Map.Entry<String, EnhancedEntity> entry : map.entrySet()) {
				log.debug("Entity => " + entry.getKey() + ", ForeignKeys => "
						+ entry.getValue().getForeignKeysMap());
			}
		}

		return new ArrayList<EnhancedEntity>(map.values());
	}
	
	private void resolveEmbeddedEntities(Object o, CascadeType cascadeType,
			Map<String, EnhancedEntity> entities)
			throws PropertyAccessException {
		EntityMetadata m =  em.getMetadataManager().getEntityMetadata(o.getClass());		

		String id = PropertyAccessorHelper.getId(o, m);

		// Ensure that @Id is set
		if (null == id || id.trim().isEmpty()) {
			throw new PersistenceException("Missing primary key >> " + m.getEntityClazz().getName() + "#"
					+ m.getIdProperty().getName());
		}


		String mapKeyForEntity = m.getEntityClazz().getName() + "_" + id;
		log.debug("Resolving >> " + mapKeyForEntity);

		// Map to hold property-name=>foreign-entity relations
		Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

		// Save to map
		entities.put(mapKeyForEntity, em.getFactory().getEnhancedEntity(o, id,
				foreignKeysMap));				
	}

	/**
	 * helper method to recursively build reachable object list.
	 * 
	 * @param o
	 *            the o
	 * @param cascadeType
	 *            the cascade type
	 * @param entities
	 *            the entities
	 * @return the all reachable entities
	 * @throws PropertyAccessException
	 *             the property access exception
	 */
	private void recursivelyResolveEntities(Object o, CascadeType cascadeType,
			Map<String, EnhancedEntity> entities)
			throws PropertyAccessException {
		
		EntityMetadata m = null;
		try {
			m = em.getMetadataManager().getEntityMetadata(o.getClass());
		} catch (Exception e) {
			// Object might already be an enhanced entity
		}

		if (m == null) {
			return;
		}	

		String id = PropertyAccessorHelper.getId(o, m);

		// Ensure that @Id is set
		if (null == id || id.trim().isEmpty()) {
			throw new PersistenceException("Missing primary key >> "
					+ m.getEntityClazz().getName() + "#"
					+ m.getIdProperty().getName());
		}

		// Dummy name to check if the object is already processed
		String mapKeyForEntity = m.getEntityClazz().getName() + "_" + id;

		if (entities.containsKey(mapKeyForEntity)) {
			return;
		}

		log.debug("Resolving >> " + mapKeyForEntity);

		// Map to hold property-name=>foreign-entity relations
		Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

		// Save to map
		entities.put(mapKeyForEntity, em.getFactory().getEnhancedEntity(o, id,
				foreignKeysMap));
		
		// Iterate over EntityMetata.Relation relations
		for (EntityMetadata.Relation relation : m.getRelations()) {

			// Cascade?
			if (!relation.getCascades().contains(CascadeType.ALL)
					&& !relation.getCascades().contains(cascadeType)) {
				continue;
			}

			// Target entity
			Class<?> targetClass = relation.getTargetEntity();
			// Mapped to this property
			Field targetField = relation.getProperty();
			// Is it optional?
			boolean optional = relation.isOptional();

			// Value
			Object value = PropertyAccessorHelper.getObject(o, targetField);

			// if object is not null, then proceed
			if (null != value) {

				if (relation.isUnary()) {
					// Unary relation will have single target object.
					String targetId = PropertyAccessorHelper.getId(value, em
							.getMetadataManager()
							.getEntityMetadata(targetClass));

					Set<String> foreignKeys = new HashSet<String>();

					foreignKeys.add(targetId);
					// put to map
					foreignKeysMap.put(targetField.getName(), foreignKeys);

					// get all other reachable objects from object "value"
					recursivelyResolveEntities(value, cascadeType, entities);

				}
				if (relation.isCollection()) {
					// Collection relation can have many target objects.

					// Value must map to Collection interface.
					@SuppressWarnings("unchecked")
					Collection collection = (Collection) value;

					Set<String> foreignKeys = new HashSet<String>();

					// Iterate over each Object and get the @Id
					for (Object o_ : collection) {
						String targetId = PropertyAccessorHelper.getId(o_, em
								.getMetadataManager().getEntityMetadata(
										targetClass));

						foreignKeys.add(targetId);

						// Get all other reachable objects from "o_"
						recursivelyResolveEntities(o_, cascadeType, entities);
					}
					foreignKeysMap.put(targetField.getName(), foreignKeys);
				}
			}

			// if the value is null
			else {
				// halt, if this was a non-optional property
				if (!optional) {
					throw new PersistenceException("Missing "
							+ targetClass.getName() + "."
							+ targetField.getName());
				}
			}
		}
	}

	/**
	 * Populate foreign entities.
	 * 
	 * @param containingEntity
	 *            the containing entity
	 * @param containingEntityId
	 *            the containing entity id
	 * @param relation
	 *            the relation
	 * @param foreignKeys
	 *            the foreign keys
	 * @throws PropertyAccessException
	 *             the property access exception
	 */
	public void populateForeignEntities (Object entity, String entityId,
			EntityMetadata.Relation relation, String... foreignKeys)
			throws PropertyAccessException {

		if (null == foreignKeys || foreignKeys.length == 0) {
			return;
		}

		String entityName = entity.getClass().getName() + "_" + entityId + "#"
				+ relation.getProperty().getName();

		log.debug("Populating foreign entities for " + entityName);

		// foreignEntityClass
		Class<?> foreignEntityClass = relation.getTargetEntity();

		// Eagerly Caching containing entity to avoid it's own loading,
		// in case the target contains a reference to containing entity.
		em.getSession().store(entity, entityId, Boolean.FALSE);

		if (relation.isUnary()) {
			// there is just one target object
			String foreignKey = foreignKeys[0];

			Object foreignObject = getForeignEntityOrProxy(entityName,
					foreignEntityClass, foreignKey, relation);

			PropertyAccessorHelper.set(entity, relation.getProperty(),
					foreignObject);
		}

		else if (relation.isCollection()) {
			// there could be multiple target objects

			// Cast to Collection
			Collection<Object> foreignObjects = null;
			if (relation.getPropertyType().equals(Set.class)) {
				foreignObjects = new HashSet<Object>();
			} else if (relation.getPropertyType().equals(List.class)) {
				foreignObjects = new ArrayList<Object>();
			}

			// Iterate over keys
			for (String foreignKey : foreignKeys) {
				Object foreignObject = getForeignEntityOrProxy(entityName,
						foreignEntityClass, foreignKey, relation);
				foreignObjects.add(foreignObject);
			}

			PropertyAccessorHelper.set(entity, relation.getProperty(),
					foreignObjects);
		}
	}

	/**
	 * Helper method to load Foreign Entity/Proxy
	 * 
	 * @param entityName
	 *            the entity name
	 * @param persistentClass
	 *            the persistent class
	 * @param foreignKey
	 *            the foreign key
	 * @param relation
	 *            the relation
	 * @return the foreign entity or proxy
	 */
	private Object getForeignEntityOrProxy(String entityName,
			Class<?> persistentClass, String foreignKey,
			EntityMetadata.Relation relation) {

		// Check in session cache!
		Object cached = em.getSession().lookup(persistentClass, foreignKey);
		if (cached != null) {
			return cached;
		}

		FetchType fetch = relation.getFetchType();

		if (fetch.equals(FetchType.EAGER)) {
			log.debug("Eagerly loading >> " + persistentClass.getName() + "_"
					+ foreignKey);
			// load target eagerly!
			return em.immediateLoadAndCache(persistentClass, foreignKey);
		} else {
			log.debug("Creating proxy for >> " + persistentClass.getName() + "#"
					+ relation.getProperty().getName() + "_" + foreignKey);

			// metadata
			EntityMetadata m = em.getMetadataManager().getEntityMetadata(
					persistentClass);

			return em.getFactory().getLazyEntity(entityName, persistentClass,
					m.getReadIdentifierMethod(), m.getWriteIdentifierMethod(),
					foreignKey, em);
		}
	}

}
