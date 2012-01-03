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
package com.impetus.kundera.persistence.handler.impl;

import java.lang.reflect.Field;

import org.hibernate.proxy.HibernateProxy;

import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class EntitySaveGraph.
 * 
 * @author vivek.mishra
 */
public class EntitySaveGraph
{

    /** The parent entity. */
    private Object parentEntity;

    /** The child entity. */
    private Object childEntity;

    /** The key name. */
    private String fKeyName;

    /** The key value. */
    private String fKeyValue;

    private boolean isSharedPrimaryKey;

    private boolean isUniDirectional = true;

    private boolean isRelatedViaJoinTable;

    private String parentId;

    private String childId;

    private Class<?> parentClass;

    private Class<?> childClass;

    private Field property;

    private Field bidirectionalProperty;

    /**
     * @param type
     */
    public EntitySaveGraph(Field type)
    {
        this.property = type;
    }

    public EntitySaveGraph()
    {

    }

    /**
     * Gets the parent entity.
     * 
     * @return the parentEntity
     */
    public Object getParentEntity()
    {
        return parentEntity;
    }

    /**
     * Sets the parent entity.
     * 
     * @param parentEntity
     *            the parentEntity to set
     */
    public void setParentEntity(Object parentEntity)
    {
        this.parentEntity = parentEntity;
    }

    /**
     * Gets the child entity.
     * 
     * @return the childEntity
     */
    public Object getChildEntity()
    {
        return childEntity;
    }

    /**
     * Sets the child entity.
     * 
     * @param childEntity
     *            the childEntity to set
     */
    public void setChildEntity(Object childEntity)
    {
        this.childEntity = childEntity;
    }

    /**
     * Gets the f key name.
     * 
     * @return the fKeyName
     */
    public String getfKeyName()
    {
        return fKeyName;
    }

    /**
     * Sets the f key name.
     * 
     * @param fKeyName
     *            the fKeyName to set
     */
    public void setfKeyName(String fKeyName)
    {
        this.fKeyName = fKeyName;
    }

    /**
     * Gets the f key value.
     * 
     * @return the fKeyValue
     */
    public String getfKeyValue()
    {
        return fKeyValue;
    }

    /**
     * Sets the f key value.
     * 
     * @param fKeyValue
     *            the fKeyValue to set
     */
    public void setfKeyValue(String fKeyValue)
    {
        this.fKeyValue = fKeyValue;
    }

    /**
     * @return the isSharedPrimaryKey
     */
    public boolean isSharedPrimaryKey()
    {
        return isSharedPrimaryKey;
    }

    /**
     * @param isSharedPrimaryKey
     *            the isSharedPrimaryKey to set
     */
    public void setSharedPrimaryKey(boolean isSharedPrimaryKey)
    {
        this.isSharedPrimaryKey = isSharedPrimaryKey;
    }

    /**
     * @return the isUniDirectional
     */
    public boolean isUniDirectional()
    {
        return isUniDirectional;
    }

    /**
     * @param isUniDirectional
     *            the isUniDirectional to set
     */
    public void setUniDirectional(boolean isUniDirectional)
    {
        this.isUniDirectional = isUniDirectional;
    }

    /**
     * @return the isRelatedViaJoinTable
     */
    public boolean isRelatedViaJoinTable()
    {
        return isRelatedViaJoinTable;
    }

    /**
     * @param isRelatedViaJoinTable
     *            the isRelatedViaJoinTable to set
     */
    public void setRelatedViaJoinTable(boolean isRelatedViaJoinTable)
    {
        this.isRelatedViaJoinTable = isRelatedViaJoinTable;
    }

    /**
     * @return the parentId
     */
    public String getParentId()
    {
        return parentId;
    }

    /**
     * @param parentId
     *            the parentId to set
     */
    public void setParentId(String parentId)
    {
        this.parentId = parentId;
    }

    /**
     * @return the childId
     */
    public String getChildId()
    {
        return childId;
    }

    /**
     * @param childId
     *            the childId to set
     */
    public void setChildId(String childId)
    {
        this.childId = childId;
    }

    /**
     * @return the isSwapped
     */

    /**
     * @return the parentClass
     */
    public Class<?> getParentClass()
    {
        if (parentClass == null)
        {
            if (parentEntity instanceof HibernateProxy)
            {
                return parentEntity.getClass().getSuperclass();
            }

            parentClass = parentEntity != null && !PropertyAccessorHelper.isCollection(parentEntity.getClass()) ? parentEntity
                    .getClass() : PropertyAccessorHelper.getGenericClass(getProperty());
        }
        return parentClass;
    }

    /**
     * @return the childClass
     */
    public Class<?> getChildClass()
    {
        if (childClass == null)
        {
            if (childEntity instanceof HibernateProxy)
            {
                return childEntity.getClass().getSuperclass();
            }
            childClass = childEntity != null && !PropertyAccessorHelper.isCollection(childEntity.getClass()) ? childEntity
                    .getClass() : PropertyAccessorHelper.getGenericClass(getProperty());
        }
        return childClass;
    }

    /**
     * @return the property
     */
    public Field getProperty()
    {
        return property;
    }

    /**
     * @param property
     *            the property to set
     */
    public void setProperty(Field property)
    {
        this.property = property;
    }

    /**
     * @return the bidirectionalProperty
     */
    public Field getBidirectionalProperty()
    {
        return bidirectionalProperty;
    }

    /**
     * @param bidirectionalProperty
     *            the bidirectionalProperty to set
     */
    public void setBidirectionalProperty(Field bidirectionalProperty)
    {
        this.bidirectionalProperty = bidirectionalProperty;
    }

}