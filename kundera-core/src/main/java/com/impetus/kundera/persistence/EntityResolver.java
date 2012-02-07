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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;


/**
 * The Class EntityReachabilityResolver.
 * 
 * @author animesh.kumar
 */
public class EntityResolver
{

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(EntityResolver.class);

    /**
     * Resolve all reachable entities from entity.
     *
     * @param entity the entity
     * @param cascadeType the cascade type
     * @param persistenceUnits the persistence units
     * @return the all reachable entities
     */
    public static List<EnhancedEntity> resolve(Object entity, CascadeType cascadeType, String... persistenceUnits)
    {
        Map<String, EnhancedEntity> map = new LinkedHashMap<String, EnhancedEntity>();
        try
        {
            LOG.debug("Resolving reachable entities for cascade " + cascadeType);

            recursivelyResolveEntities(entity, cascadeType, map, persistenceUnits);

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
     * @param object the o
     * @param cascadeType the cascade type
     * @param entities the entities
     * @param persistenceUnits the persistence units
     * @return the all reachable entities
     * @throws PropertyAccessException the property access exception
     */
    private static void recursivelyResolveEntities(Object object, CascadeType cascadeType,
            Map<String, EnhancedEntity> entities, String... persistenceUnits) throws PropertyAccessException
    {

        EntityMetadata entityMetaData = null;
        try
        {
            entityMetaData = KunderaMetadataManager.getEntityMetadata(object.getClass(), persistenceUnits);
        }
        catch (Exception e)
        {
            // Object might already be an enhanced entity
        }

        if (entityMetaData == null)
        {
            return;
        }

        String id = PropertyAccessorHelper.getId(object, entityMetaData);

        // Ensure that @Id is set
        if (null == id || id.trim().isEmpty())
        {
            throw new PersistenceException("Missing primary key >> " + entityMetaData.getEntityClazz().getName() + "#"
                    + entityMetaData.getIdColumn().getField().getName());
        }

        // Dummy name to check if the object is already processed
        String mapKeyForEntity = entityMetaData.getEntityClazz().getName() + "_" + id;

        if (entities.containsKey(mapKeyForEntity))
        {
            return;
        }

        LOG.debug("Resolving >> " + mapKeyForEntity);

        // Map to hold property-name=>foreign-entity relations
        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        // Save to map
        entities.put(mapKeyForEntity, getEnhancedEntity(object, id, foreignKeysMap));

        // Iterate over EntityMetata.Relation relations
        for (Relation relation : entityMetaData.getRelations())
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
            Object value = PropertyAccessorHelper.getObject(object, targetField);

            // if object is not null, then proceed
            if (null != value)
            {
                EntityMetadata relMetadata = KunderaMetadataManager.getEntityMetadata(targetClass, persistenceUnits);

                if (relation.isUnary())
                {
                    // Unary relation will have single target object.
                    String targetId = PropertyAccessorHelper.getId(value, relMetadata);

                    Set<String> foreignKeys = new HashSet<String>();

                    foreignKeys.add(targetId);
                    // put to map
                    foreignKeysMap.put(targetField.getName(), foreignKeys);

                    // get all other reachable objects from object "value"
                    recursivelyResolveEntities(value, cascadeType, entities, persistenceUnits);

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
                        recursivelyResolveEntities(o_, cascadeType, entities, persistenceUnits);
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
    public static EnhancedEntity getEnhancedEntity(Object entity, String id, Map<String, Set<String>> foreignKeyMap)
    {
        return KunderaMetadataManager.getEntityEnhancerFactory().getProxy(entity, id, foreignKeyMap);
    }
}
