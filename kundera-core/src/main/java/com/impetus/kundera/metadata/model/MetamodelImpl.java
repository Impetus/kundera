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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Metamodel;


/**
 * The Class MetamodelImpl.
 *
 * @author amresh.singh
 */
public class MetamodelImpl implements Metamodel
{
    
    /** The entity metadata map. */
    Map<Class<?>, EntityMetadata> entityMetadataMap;

    /** The entity name to class map. */
    Map<String, Class<?>> entityNameToClassMap;

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#entity(java.lang.Class)
     */
    @Override
    public <X> EntityType<X> entity(Class<X> paramClass)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#managedType(java.lang.Class)
     */
    @Override
    public <X> ManagedType<X> managedType(Class<X> paramClass)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#embeddable(java.lang.Class)
     */
    @Override
    public <X> EmbeddableType<X> embeddable(Class<X> paramClass)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#getManagedTypes()
     */
    @Override
    public Set<ManagedType<?>> getManagedTypes()
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#getEntities()
     */
    @Override
    public Set<EntityType<?>> getEntities()
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Metamodel#getEmbeddables()
     */
    @Override
    public Set<EmbeddableType<?>> getEmbeddables()
    {
        return null;
    }

    /**
     * Instantiates a new metamodel impl.
     */
    public MetamodelImpl()
    {
        super();
        setEntityMetadataMap(new HashMap<Class<?>, EntityMetadata>());
    }

    /**
     * Gets the entity metadata map.
     *
     * @return the entityMetadataMap
     */
    public Map<Class<?>, EntityMetadata> getEntityMetadataMap()
    {
        if (entityMetadataMap == null)
        {
            entityMetadataMap = new HashMap<Class<?>, EntityMetadata>();
        }
        return entityMetadataMap;
    }

    /**
     * Sets the entity metadata map.
     *
     * @param entityMetadataMap the entityMetadataMap to set
     */
    public void setEntityMetadataMap(Map<Class<?>, EntityMetadata> entityMetadataMap)
    {
        this.entityMetadataMap = entityMetadataMap;
    }

    /**
     * Adds the entity metadata.
     *
     * @param clazz the clazz
     * @param entityMetadata the entity metadata
     */
    public void addEntityMetadata(Class<?> clazz, EntityMetadata entityMetadata)
    {
        getEntityMetadataMap().put(clazz, entityMetadata);
    }

    /**
     * Gets the entity metadata.
     *
     * @param entityClass the entity class
     * @return the entity metadata
     */
    public EntityMetadata getEntityMetadata(Class<?> entityClass)
    {
        return getEntityMetadataMap().get(entityClass);
    }

    /**
     * Gets the entity name to class map.
     *
     * @return the entityNameToClassMap
     */
    public Map<String, Class<?>> getEntityNameToClassMap()
    {
        if (entityNameToClassMap == null)
        {
            entityNameToClassMap = new HashMap<String, Class<?>>();
        }
        return entityNameToClassMap;
    }

    /**
     * Sets the entity name to class map.
     *
     * @param entityNameToClassMap the entityNameToClassMap to set
     */
    public void setEntityNameToClassMap(Map<String, Class<?>> entityNameToClassMap)
    {
        this.entityNameToClassMap = entityNameToClassMap;
    }

    /**
     * Adds the entity name to class mapping.
     *
     * @param className the class name
     * @param entityClass the entity class
     */
    public void addEntityNameToClassMapping(String className, Class<?> entityClass)
    {
        getEntityNameToClassMap().put(className, entityClass);
    }

    /**
     * Gets the entity class.
     *
     * @param className the class name
     * @return the entity class
     */
    public Class<?> getEntityClass(String className)
    {
        return getEntityNameToClassMap().get(className);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return entityMetadataMap.toString();
    }

}
