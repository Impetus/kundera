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
package com.impetus.kundera.startup.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Metamodel;

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * @author amresh.singh
 * 
 */
public class MetamodelImpl implements Metamodel
{
    Map<Class<?>, EntityMetadata> entityMetadataMap;

    Map<String, Class<?>> entityNameToClassMap;

    @Override
    public <X> EntityType<X> entity(Class<X> paramClass)
    {
        return null;
    }

    @Override
    public <X> ManagedType<X> managedType(Class<X> paramClass)
    {
        return null;
    }

    @Override
    public <X> EmbeddableType<X> embeddable(Class<X> paramClass)
    {
        return null;
    }

    @Override
    public Set<ManagedType<?>> getManagedTypes()
    {
        return null;
    }

    @Override
    public Set<EntityType<?>> getEntities()
    {
        return null;
    }

    @Override
    public Set<EmbeddableType<?>> getEmbeddables()
    {
        return null;
    }

    public MetamodelImpl()
    {
        super();
        setEntityMetadataMap(new HashMap<Class<?>, EntityMetadata>());
    }

    /**
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
     * @param entityMetadataMap
     *            the entityMetadataMap to set
     */
    public void setEntityMetadataMap(Map<Class<?>, EntityMetadata> entityMetadataMap)
    {
        this.entityMetadataMap = entityMetadataMap;
    }

    /**
     * @param entityMetadataMap
     *            the entityMetadataMap to set
     */
    public void addEntityMetadata(Class<?> clazz, EntityMetadata entityMetadata)
    {
        getEntityMetadataMap().put(clazz, entityMetadata);
    }

    public EntityMetadata getEntityMetadata(Class<?> entityClass)
    {
        return getEntityMetadataMap().get(entityClass);
    }

    /**
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
     * @param entityNameToClassMap
     *            the entityNameToClassMap to set
     */
    public void setEntityNameToClassMap(Map<String, Class<?>> entityNameToClassMap)
    {
        this.entityNameToClassMap = entityNameToClassMap;
    }

    public void addEntityNameToClassMapping(String className, Class<?> entityClass)
    {
        getEntityNameToClassMap().put(className, entityClass);
    }

    public Class<?> getEntityClass(String className)
    {
        return getEntityNameToClassMap().get(className);
    }

    public String toString()
    {
        return entityMetadataMap.toString();
    }

}
