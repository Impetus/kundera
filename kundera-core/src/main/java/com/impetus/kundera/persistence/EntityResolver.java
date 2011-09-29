/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.persistence;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.DBType;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.proxy.EntityEnhancerFactory;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializerFactory;
import com.impetus.kundera.proxy.cglib.CglibEntityEnhancerFactory;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;

/**
 * The Class EntityReachabilityResolver.
 * 
 * @author animesh.kumar
 */
public class EntityResolver
{

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(EntityResolver.class);

    /** The em. */
    private EntityManagerImpl em;

    /** The enhanced proxy factory. */
    private EntityEnhancerFactory enhancedProxyFactory;

    /** The lazy initializer factory. */
    private LazyInitializerFactory lazyInitializerFactory;

    /**
     * Instantiates a new entity resolver.
     * 
     * @param em
     *            the em
     */
    public EntityResolver(EntityManagerImpl em)
    {
        this.em = em;
        enhancedProxyFactory = new CglibEntityEnhancerFactory();
        lazyInitializerFactory = new CglibLazyInitializerFactory();
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
    public List<EnhancedEntity> resolve(Object entity, CascadeType cascadeType, DBType dbType)
    {
        Map<String, EnhancedEntity> map = new LinkedHashMap<String, EnhancedEntity>();
        try
        {
            LOG.debug("Resolving reachable entities for cascade " + cascadeType);

            recursivelyResolveEntities(entity, cascadeType, map);

        }
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e.getMessage());
        }

        if (LOG.isDebugEnabled())
        {
            for (Map.Entry<String, EnhancedEntity> entry : map.entrySet())
            {
                LOG.debug("Entity => " + entry.getKey() + ", ForeignKeys => " + entry.getValue().getForeignKeysMap());
            }
        }

        return new ArrayList<EnhancedEntity>(map.values());
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
    private void recursivelyResolveEntities(Object o, CascadeType cascadeType, Map<String, EnhancedEntity> entities)
            throws PropertyAccessException
    {

        EntityMetadata m = null;
        try
        {
            m = ((MetamodelImpl) em.getEntityManagerFactory().getMetamodel()).getEntityMetadata(o.getClass());
        }
        catch (Exception e)
        {
            // Object might already be an enhanced entity
        }

        if (m == null)
        {
            return;
        }

        String id = PropertyAccessorHelper.getId(o, m);

        // Ensure that @Id is set
        if (null == id || id.trim().isEmpty())
        {
            throw new PersistenceException("Missing primary key >> " + m.getEntityClazz().getName() + "#"
                    + m.getIdColumn().getField().getName());
        }

        // Dummy name to check if the object is already processed
        String mapKeyForEntity = m.getEntityClazz().getName() + "_" + id;

        if (entities.containsKey(mapKeyForEntity))
        {
            return;
        }

        LOG.debug("Resolving >> " + mapKeyForEntity);

        // Map to hold property-name=>foreign-entity relations
        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        // Save to map
        entities.put(mapKeyForEntity, getEnhancedEntity(o, id, foreignKeysMap));

        // Iterate over EntityMetata.Relation relations
        for (Relation relation : m.getRelations())
        {

            // Cascade?
            if (!relation.getCascades().contains(CascadeType.ALL) && !relation.getCascades().contains(cascadeType))
            {
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
            if (null != value)
            {
                EntityMetadata relMetadata = ((MetamodelImpl) em.getEntityManagerFactory().getMetamodel())
                        .getEntityMetadataMap().get(targetClass);

                if (relation.isUnary())
                {
                    // Unary relation will have single target object.
                    String targetId = PropertyAccessorHelper.getId(value, relMetadata);

                    Set<String> foreignKeys = new HashSet<String>();

                    foreignKeys.add(targetId);
                    // put to map
                    foreignKeysMap.put(targetField.getName(), foreignKeys);

                    // get all other reachable objects from object "value"
                    recursivelyResolveEntities(value, cascadeType, entities);

                }
                if (relation.isCollection())
                {
                    // Collection relation can have many target objects.

                    // Value must map to Collection interface.
                    @SuppressWarnings("unchecked")
                    Collection collection = (Collection) value;

                    Set<String> foreignKeys = new HashSet<String>();

                    // Iterate over each Object and get the @Id
                    for (Object o_ : collection)
                    {
                        String targetId = PropertyAccessorHelper.getId(o_, relMetadata);

                        foreignKeys.add(targetId);

                        // Get all other reachable objects from "o_"
                        recursivelyResolveEntities(o_, cascadeType, entities);
                    }
                    foreignKeysMap.put(targetField.getName(), foreignKeys);
                }
            }

            // if the value is null
            else
            {
                // halt, if this was a non-optional property
                if (!optional)
                {
                    throw new PersistenceException("Missing " + targetClass.getName() + "." + targetField.getName());
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
    public void populateForeignEntities(Object entity, String entityId, Relation relation, String... foreignKeys)
            throws PropertyAccessException
    {

        if (null == foreignKeys || foreignKeys.length == 0)
        {
            return;
        }

        String entityName = entity.getClass().getName() + "_" + entityId + "#" + relation.getProperty().getName();

        LOG.debug("Populating foreign entities for " + entityName);

        // foreignEntityClass
        Class<?> foreignEntityClass = relation.getTargetEntity();

        // Eagerly Caching containing entity to avoid it's own loading,
        // in case the target contains a reference to containing entity.
        em.getSession().store(entity, entityId, Boolean.FALSE);

        EntityMetadata relMetadata = ((MetamodelImpl) em.getEntityManagerFactory().getMetamodel())
                .getEntityMetadata(foreignEntityClass);

        // Check for cross-store persistence
        if (relMetadata.getPersistenceUnit() == null
                || em.getPersistenceUnitName().equals(relMetadata.getPersistenceUnit()))
        {
            populateForeignEntityFromSameDatastore(entity, relation, entityName, foreignEntityClass, foreignKeys);
        }
        else
        {
            LOG.debug("Relationship entity is for a different database, only PKs will be set");
            populateForeignEntityFromDifferentDatastore(entity, relation, entityName, foreignEntityClass, foreignKeys);
        }
    }

    /**
     * Populates entire foreign entity object (Because parent entity and foreign
     * entity are in the same datastore)
     */
    private void populateForeignEntityFromSameDatastore(Object entity, Relation relation, String entityName,
            Class<?> foreignEntityClass, String... foreignKeys) throws PropertyAccessException
    {
        if (relation.isUnary())
        {
            // there is just one target object
            String foreignKey = foreignKeys[0];

            Object foreignObject = getForeignEntityOrProxy(entityName, foreignEntityClass, foreignKey, relation);

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObject);
        }

        else if (relation.isCollection())
        {
            // there could be multiple target objects Cast to Collection
            Collection<Object> foreignObjects = null;
            if (relation.getPropertyType().equals(Set.class))
            {
                foreignObjects = new HashSet<Object>();
            }
            else if (relation.getPropertyType().equals(List.class))
            {
                foreignObjects = new ArrayList<Object>();
            }

            // Iterate over keys
            for (String foreignKey : foreignKeys)
            {
                Object foreignObject = getForeignEntityOrProxy(entityName, foreignEntityClass, foreignKey, relation);
                foreignObjects.add(foreignObject);
            }

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObjects);
        }
    }

    /**
     * Populates only row key in the foreign entity object (Because foreign
     * entity is in different datastore vis-a-vis parent entity)
     */
    private void populateForeignEntityFromDifferentDatastore(Object entity, Relation relation, String entityName,
            Class<?> foreignEntityClass, String... foreignKeys) throws PropertyAccessException
    {

        EntityMetadata relMetadata = ((MetamodelImpl) em.getEntityManagerFactory().getMetamodel())
                .getEntityMetadata(foreignEntityClass);
        this.em = (EntityManagerImpl) Persistence.createEntityManagerFactory(relMetadata.getPersistenceUnit())
                .createEntityManager();

        if (relation.isUnary())
        {
            // there is just one target object
            String foreignKey = foreignKeys[0];

            Object foreignObject = getForeignEntityOrProxy(entityName, foreignEntityClass, foreignKey, relation);

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObject);
        }

        else if (relation.isCollection())
        {
            // there could be multiple target objects

            // Cast to Collection
            Collection<Object> foreignObjects = null;
            if (relation.getPropertyType().equals(Set.class))
            {
                foreignObjects = new HashSet<Object>();
            }
            else if (relation.getPropertyType().equals(List.class))
            {
                foreignObjects = new ArrayList<Object>();
            }

            // Iterate over keys
            for (String foreignKey : foreignKeys)
            {
                Object foreignObject = getForeignEntityOrProxy(entityName, foreignEntityClass, foreignKey, relation);
                foreignObjects.add(foreignObject);
            }

            PropertyAccessorHelper.set(entity, relation.getProperty(), foreignObjects);
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
    private Object getForeignEntityOrProxy(String entityName, Class<?> persistentClass, String foreignKey,
            Relation relation)
    {

        // Check in session cache!
        Object cached = em.getSession().lookup(persistentClass, foreignKey);
        if (cached != null)
        {
            return cached;
        }

        FetchType fetch = relation.getFetchType();

        if (fetch.equals(FetchType.EAGER))
        {
            LOG.debug("Eagerly loading >> " + persistentClass.getName() + "_" + foreignKey);
            // load target eagerly!
            return em.immediateLoadAndCache(persistentClass, foreignKey);
        }
        else
        {
            LOG.debug("Creating proxy for >> " + persistentClass.getName() + "#" + relation.getProperty().getName()
                    + "_" + foreignKey);

            // metadata
            EntityMetadata m = ((MetamodelImpl) em.getEntityManagerFactory().getMetamodel())
                    .getEntityMetadata(persistentClass);

            return getLazyEntity(entityName, persistentClass, m.getReadIdentifierMethod(),
                    m.getWriteIdentifierMethod(), foreignKey, em);
        }
    }

    /**
     * Gets the lazy entity.
     * 
     * @param entityName
     *            the entity name
     * @param persistentClass
     *            the persistent class
     * @param getIdentifierMethod
     *            the get identifier method
     * @param setIdentifierMethod
     *            the set identifier method
     * @param id
     *            the id
     * @param em
     *            the em
     * @return the lazy entity
     */
    private KunderaProxy getLazyEntity(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, String id, EntityManagerImpl em)
    {
        return lazyInitializerFactory.getProxy(entityName, persistentClass, getIdentifierMethod, setIdentifierMethod,
                id, em);
    }

    /**
     * Gets the enhanced entity.
     * 
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param foreignKeyMap
     *            the foreign key map
     * @return the enhanced entity
     */
    private EnhancedEntity getEnhancedEntity(Object entity, String id, Map<String, Set<String>> foreignKeyMap)
    {
        return enhancedProxyFactory.getProxy(entity, id, foreignKeyMap);
    }

}
