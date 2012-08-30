/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Metamodel;

import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;

/**
 * The Class MetamodelImpl implements <code> MetaModel</code> interface.
 * Responsible for holding reference to complete metamodel and relative
 * information, in the form of: a) EntityTypes b) Embeddables
 * c)MappedSuperClassTypes
 * 
 * @author vivek.mishra
 */
public class MetamodelImpl implements Metamodel
{

    /** The entity metadata map. */
    Map<Class<?>, EntityMetadata> entityMetadataMap;

    /** The entity name to class map. */
    Map<String, Class<?>> entityNameToClassMap;

    /** The managed types. */
    private Map<Class<?>, EntityType<?>> entityTypes;

    /** The embeddables. */
    private Map<Class<?>, ManagedType<?>> embeddables;

    /** The mapped super class types. */
    private Map<Class<?>, ManagedType<?>> mappedSuperClassTypes;

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#entity(java.lang.Class)
     */
    @Override
    public <X> EntityType<X> entity(Class<X> paramClass)
    {
        EntityType entityType = entityTypes.get(paramClass);
        if (entityType == null)
        {
            throw new IllegalArgumentException("Not an entity, {class:" + paramClass + "}");
        }
        return entityType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#managedType(java.lang.Class)
     */
    @Override
    public <X> ManagedType<X> managedType(Class<X> paramClass)
    {
        ManagedType managedType = entityTypes.get(paramClass);
        if (managedType == null)
        {
            managedType = embeddables.get(paramClass);
            if (managedType == null)
            {
                managedType = mappedSuperClassTypes.get(paramClass);
            }
        }

        if (managedType == null)
        {
            throw new IllegalArgumentException("Not a managed type, {class: " + paramClass + "}");
        }
        return managedType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#embeddable(java.lang.Class)
     */
    @Override
    public <X> EmbeddableType<X> embeddable(Class<X> paramClass)
    {
        EmbeddableType embeddableType = (EmbeddableType) embeddables.get(paramClass);
        if (embeddableType == null)
        {
            throw new IllegalArgumentException("Not a embeddable type, {class: " + paramClass + "}");
        }

        return embeddableType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#getManagedTypes()
     */
    @Override
    public Set<ManagedType<?>> getManagedTypes()
    {
        Set<ManagedType<?>> managedTypeCollection = new HashSet<ManagedType<?>>();
        if (entityTypes != null)
        {
            managedTypeCollection.addAll(entityTypes.values());
        }
        if (embeddables != null)
        {
            managedTypeCollection.addAll((Collection<? extends ManagedType<?>>) embeddables.values());
        }
        if (mappedSuperClassTypes != null)
        {
            managedTypeCollection.addAll((Collection<? extends ManagedType<?>>) mappedSuperClassTypes.values());
        }
        return managedTypeCollection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#getEntities()
     */
    @Override
    public Set<EntityType<?>> getEntities()
    {
        Set<EntityType<?>> entities = null;
        if (entityTypes != null)
        {
            entities = new HashSet<EntityType<?>>(entityTypes.values());
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Metamodel#getEmbeddables()
     */
    @Override
    public Set<EmbeddableType<?>> getEmbeddables()
    {
        Set embeddableEntities = null;
        if (embeddables != null)
        {
            embeddableEntities = new HashSet(embeddables.values());
        }
        return embeddableEntities;
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
     * @param entityMetadataMap
     *            the entityMetadataMap to set
     */
    public void setEntityMetadataMap(Map<Class<?>, EntityMetadata> entityMetadataMap)
    {
        this.entityMetadataMap = entityMetadataMap;
    }

    /**
     * Adds the entity metadata.
     * 
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     */
    public void addEntityMetadata(Class<?> clazz, EntityMetadata entityMetadata)
    {
        getEntityMetadataMap().put(clazz, entityMetadata);
    }

    /**
     * Gets the entity metadata.
     * 
     * @param entityClass
     *            the entity class
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
     * @param entityNameToClassMap
     *            the entityNameToClassMap to set
     */
    public void setEntityNameToClassMap(Map<String, Class<?>> entityNameToClassMap)
    {
        this.entityNameToClassMap = entityNameToClassMap;
    }

    /**
     * Adds the entity name to class mapping.
     * 
     * @param className
     *            the class name
     * @param entityClass
     *            the entity class
     */
    public void addEntityNameToClassMapping(String className, Class<?> entityClass)
    {
        getEntityNameToClassMap().put(className, entityClass);
    }

    /**
     * Gets the entity class.
     * 
     * @param className
     *            the class name
     * @return the entity class
     */
    public Class<?> getEntityClass(String className)
    {
        return getEntityNameToClassMap().get(className);
    }
    

    /**
     * Assign to managedTypes.
     * 
     * @param managedTypes
     *            the managedTypes to set
     */
    public void assignManagedTypes(Map<Class<?>, EntityType<?>> managedTypes)
    {
        if (this.entityTypes == null)
        {
            this.entityTypes = managedTypes;
        }
        else
        {
            this.entityTypes.putAll(managedTypes);
        }
    }

    /**
     * Assign embeddables to embeddables collection.
     * @param embeddables
     *            the embeddables to set
     */
    public void assignEmbeddables(Map<Class<?>, ManagedType<?>> embeddables)
    {
        if (this.embeddables == null)
        {
            this.embeddables = embeddables;
        }
        else
        {
            this.embeddables.putAll(embeddables);
        }
    }

    /**
     * Adds mapped super class to mapped super class collection.
     * @param mappedSuperClass
     *            the mappedSuperClassTypes to set
     */
    public void assignMappedSuperClass(Map<Class<?>, ManagedType<?>> mappedSuperClass)
    {
        if (this.mappedSuperClassTypes == null)
        {
            this.mappedSuperClassTypes = mappedSuperClass;
        }
        else
        {
            this.mappedSuperClassTypes.putAll(mappedSuperClassTypes);
        }
    }

    /**
     * Returns true, if attribute is embeddable.
     * 
     * @param embeddableClazz           class for embeddable type attribute.
     * @return try, if paramterized class is of embeddable java type.
     */
    public boolean isEmbeddable(Class embeddableClazz)
    {
        return embeddables != null ? embeddables.containsKey(embeddableClazz) : false;
    }

    /**
     * Returns entity attribute for given managed entity class.
     * 
     * @param clazz            Entity class
     * @param fieldName        field name
     * @return                 entity attribute
     */
    public Attribute getEntityAttribute(Class clazz, String fieldName)
    {
        if (entityTypes != null && entityTypes.containsKey(clazz))
        {
            EntityType entityType = entityTypes.get(clazz);
            return entityType.getAttribute(fieldName);
        }

        throw new IllegalArgumentException("No entity found: " + clazz);
    }

    /**
     * Custome implementation to offer map of embeddables available for given entityType.
     * 
     * @param clazz entity class
     * @return map of holding {@link EmbeddableType} as value and attribute name as key.
     */
    public Map<String, EmbeddableType> getEmbeddables(Class clazz)
    {
        Map<String, EmbeddableType> embeddableAttibutes = new HashMap<String, EmbeddableType>();

        if (entityTypes != null)
        {
            EntityType entity = entityTypes.get(clazz);
            Iterator<Attribute> iter = entity.getAttributes().iterator();
            while (iter.hasNext())
            {
                Attribute attribute = iter.next();
                if (isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                {
                    embeddableAttibutes.put(attribute.getName(),
                            embeddable(((AbstractAttribute) attribute).getBindableJavaType()));
                }

            }

        }
        return embeddableAttibutes;
    }
}
