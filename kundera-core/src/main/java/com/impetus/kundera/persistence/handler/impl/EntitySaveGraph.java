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

import java.beans.PersistenceDelegate;
import java.lang.reflect.Field;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.hibernate.proxy.HibernateProxy;

import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;

// TODO: Auto-generated Javadoc
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

    /** Foreign Key name for Parent -> Child. */
    private String fKeyName;

    /** Foreign Key value for Parent -> Child. */
    private String fKeyValue;

    /** Foreign Key name for Parent -> Its Parent. */
    private String revFKeyName;

    /** Foreign Key value for Parent -> Its Parent. */
    private String revFKeyValue;

    /** If this entity was child of another entity (Transitive persistence case), class of its parent. */
    private Class<?> revParentClass;

    /** The is shared primary key. */
    private boolean isSharedPrimaryKey;

    /** The is uni directional. */
    private boolean isUniDirectional = true;

    /** The is related via join table. */
    private boolean isRelatedViaJoinTable;

    /** The parent id. */
    private String parentId;

    /** The child id. */
    private String childId;

    /** The parent class. */
    private Class<?> parentClass;

    /** The child class. */
    private Class<?> childClass;

    /** The property. */
    private Field property;

    /** The bidirectional property. */
    private Field bidirectionalProperty;

    /** The isswapped. */
    private boolean isswapped;

    /**
     * Instantiates a new entity save graph.
     *
     * @param type the type
     */
    public EntitySaveGraph(Field type)
    {
        this.property = type;
    }

    /**
     * Instantiates a new entity save graph.
     */
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
        return getParentId();
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
     * Gets the rev f key name.
     *
     * @return the revFKeyName
     */
    public String getRevFKeyName()
    {
        return revFKeyName;
    }

    /**
     * Sets the rev f key name.
     *
     * @param revFKeyName the revFKeyName to set
     */
    public void setRevFKeyName(String revFKeyName)
    {
        this.revFKeyName = revFKeyName;
    }

    /**
     * Gets the rev f key value.
     *
     * @return the revFKeyValue
     */
    public String getRevFKeyValue()
    {
        return revFKeyValue;
    }

    /**
     * Sets the rev f key value.
     *
     * @param revFKeyValue the revFKeyValue to set
     */
    public void setRevFKeyValue(String revFKeyValue)
    {
        this.revFKeyValue = revFKeyValue;
    }

    /**
     * Gets the rev parent class.
     *
     * @return the revParentClass
     */
    public Class<?> getRevParentClass()
    {
        return revParentClass;
    }

    /**
     * Sets the rev parent class.
     *
     * @param revParentClass the revParentClass to set
     */
    public void setRevParentClass(Class<?> revParentClass)
    {
        this.revParentClass = revParentClass;
    }

    /**
     * Checks if is shared primary key.
     *
     * @return the isSharedPrimaryKey
     */
    public boolean isSharedPrimaryKey()
    {
        return isSharedPrimaryKey;
    }

    /**
     * Sets the shared primary key.
     *
     * @param isSharedPrimaryKey the isSharedPrimaryKey to set
     */
    public void setSharedPrimaryKey(boolean isSharedPrimaryKey)
    {
        this.isSharedPrimaryKey = isSharedPrimaryKey;
    }

    /**
     * Checks if is uni directional.
     *
     * @return the isUniDirectional
     */
    public boolean isUniDirectional()
    {
        return isUniDirectional;
    }

    /**
     * Sets the uni directional.
     *
     * @param isUniDirectional the isUniDirectional to set
     */
    public void setUniDirectional(boolean isUniDirectional)
    {
        this.isUniDirectional = isUniDirectional;
    }

    /**
     * Checks if is related via join table.
     *
     * @return the isRelatedViaJoinTable
     */
    public boolean isRelatedViaJoinTable()
    {
        return isRelatedViaJoinTable;
    }

    /**
     * Sets the related via join table.
     *
     * @param isRelatedViaJoinTable the isRelatedViaJoinTable to set
     */
    public void setRelatedViaJoinTable(boolean isRelatedViaJoinTable)
    {
        this.isRelatedViaJoinTable = isRelatedViaJoinTable;
    }

    /**
     * Gets the parent id.
     *
     * @return the parentId
     */
    public String getParentId()
    {
        return parentId;
    }

    /**
     * Sets the parent id.
     *
     * @param parentId the parentId to set
     */
    public void setParentId(String parentId)
    {
        this.parentId = parentId;
    }

    /**
     * Gets the child id.
     *
     * @return the childId
     */
    public String getChildId()
    {
        return childId;
    }

    /**
     * Sets the child id.
     *
     * @param childId the childId to set
     */
    public void setChildId(String childId)
    {
        this.childId = childId;
    }

    /**
     * Gets the parent class.
     *
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
     * Gets the child class.
     *
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
     * Gets the property.
     *
     * @return the property
     */
    public Field getProperty()
    {
        return property;
    }

    /**
     * Sets the property.
     *
     * @param property the property to set
     */
    public void setProperty(Field property)
    {
        this.property = property;
    }

    /**
     * Gets the bidirectional property.
     *
     * @return the bidirectionalProperty
     */
    public Field getBidirectionalProperty()
    {
        return bidirectionalProperty;
    }

    /**
     * Sets the bidirectional property.
     *
     * @param bidirectionalProperty the bidirectionalProperty to set
     */
    public void setBidirectionalProperty(Field bidirectionalProperty)
    {
        this.bidirectionalProperty = bidirectionalProperty;
    }

    /**
     * Checks if is isswapped.
     *
     * @return the isswapped
     */
    public boolean isIsswapped()
    {
        return isswapped;
    }

    /**
     * Sets the isswapped.
     *
     * @param isswapped the isswapped to set
     */
    public void setIsswapped(boolean isswapped)
    {
        this.isswapped = isswapped;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\tparentClass:");
        strBuilder.append(getParentClass());
        strBuilder.append("\n");
        strBuilder.append("\tchildClass:");
        strBuilder.append(getChildClass());
        strBuilder.append("\n");
        strBuilder.append("\tfKeyName:");
        strBuilder.append(fKeyName);
        strBuilder.append("\n");
        strBuilder.append("\tfKeyValue:");
        strBuilder.append(fKeyValue);
        strBuilder.append("\n");
        strBuilder.append("\trevFKeyName:");
        strBuilder.append(revFKeyName);
        strBuilder.append("\n");
        strBuilder.append("\trevFKeyValue:");
        strBuilder.append(revFKeyValue);        
        strBuilder.append("\n");
        strBuilder.append("\trevParentClass:");
        strBuilder.append(revParentClass);
        strBuilder.append("\n");
        strBuilder.append("\tisSharedPrimaryKey:");
        strBuilder.append(isSharedPrimaryKey);
        strBuilder.append("\n");
        strBuilder.append("\tisUniDirectional:");
        strBuilder.append(isUniDirectional);
        strBuilder.append("\n");
        strBuilder.append("\tisRelatedViaJoinTable:");
        strBuilder.append(isRelatedViaJoinTable);
        strBuilder.append("\n");
        strBuilder.append("\tparentId:");
        strBuilder.append(parentId);
        strBuilder.append("\n");
        strBuilder.append("\tchildId:");
        strBuilder.append(childId);
        strBuilder.append("\n");
        strBuilder.append("\tproperty:");
        strBuilder.append(property != null ? property.getType() : null);
        strBuilder.append("\n");
        strBuilder.append("\tbidirectionalProperty:");
        strBuilder.append(bidirectionalProperty != null ? bidirectionalProperty.getType() : null);
        strBuilder.append("\n");
        strBuilder.append("\tisswapped:");
        strBuilder.append(isswapped);
        strBuilder.append("\n");
        return strBuilder.toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    /**
     * Purpose to override this is to handle bi direction scenario only As of now. 
     * @see PersistenceDelegator @{persistOneChildEntity} method.
     * Any change to this method will broke it. 
     *  In case it is required to compare exact clone. please create a new method to handle bi-directional.
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj.getClass().isAssignableFrom(EntitySaveGraph.class))
        {
            EntitySaveGraph g = (EntitySaveGraph) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(getParentClass(), g.getParentClass());
            equalsBuilder.append(getChildClass(), g.getChildClass());
           /* equalsBuilder.append(this.fKeyName, g.getfKeyName());
            equalsBuilder.append(this.fKeyValue, g.getfKeyValue());
            equalsBuilder.append(this.revFKeyName, g.getRevFKeyName());
            equalsBuilder.append(this.revFKeyValue, g.getRevFKeyValue());
            equalsBuilder.append(this.revParentClass, g.getRevParentClass());
            equalsBuilder.append(this.isSharedPrimaryKey, g.isSharedPrimaryKey());
            equalsBuilder.append(this.isUniDirectional, g.isUniDirectional());
            equalsBuilder.append(this.isRelatedViaJoinTable, g.isRelatedViaJoinTable());
            equalsBuilder.append(this.parentId, g.getParentId());
            equalsBuilder.append(this.childId, g.getChildId());
             //this is to handle bi directional scenario. as it will be exact opposite of what it is.
            equalsBuilder.append(this.property, g.getBidirectionalProperty());
            equalsBuilder.append(this.bidirectionalProperty, g.getProperty());
            equalsBuilder.append(this.isswapped, g.isIsswapped());*/
            return equalsBuilder.isEquals();
        }
        
        return false;
    }


    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

}