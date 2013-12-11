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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class Relation.
 */
public final class Relation
{

    /** The property. */
    private Field property;

    /** The target entity. */
    private Class<?> targetEntity;

    /** If relation is through a Map, Map Key Join Class */
    private Class<?> mapKeyJoinClass;

    /** The property type. */
    private Class<?> propertyType;

    /** The fetch type. */
    FetchType fetchType;

    /** The cascades. */
    private List<CascadeType> cascades;

    /** The optional. */
    private boolean optional;

    /** The mapped by. */
    private String mappedBy;

    /** The type. */
    private Relation.ForeignKey type;

    /** Join column name for this relationship. */
    private String joinColumnName;

    /** Whether this relationship is through a Join Table. */
    private boolean isRelatedViaJoinTable;

    /** Metadata for JoinTable, applicable only if isRelatedViaJoinTable==true. */
    private JoinTableMetadata joinTableMetadata;

    /**
     * Whether this relationship is joined by primary key, only applicable for
     * OneToOne.
     */
    private boolean isJoinedByPrimaryKey;

    private Field biDirectionalField; 
    /**
     * 
     * The Enum ForeignKey.
     */
    public static enum ForeignKey
    {
        /** The ON e_ t o_ one. */
        ONE_TO_ONE,
        /** The ON e_ t o_ many. */
        ONE_TO_MANY,
        /** The MAN y_ t o_ one. */
        MANY_TO_ONE,
        /** The MAN y_ t o_ many. */
        MANY_TO_MANY
    }

    /**
     * Instantiates a new relation.
     * 
     * @param property
     *            the property
     * @param targetEntity
     *            the target entity
     * @param propertyType
     *            the property type
     * @param fetchType
     *            the fetch type
     * @param cascades
     *            the cascades
     * @param optional
     *            the optional
     * @param mappedBy
     *            the mapped by
     * @param type
     *            the type
     */

    /**
     * Specifies the type of the metadata.
     */

    public Relation(Field property, Class<?> targetEntity, Class<?> propertyType, FetchType fetchType,
            List<CascadeType> cascades, boolean optional, String mappedBy, Relation.ForeignKey type)
    {
        super();
        this.property = property;
        this.targetEntity = targetEntity;
        this.propertyType = propertyType;
        this.fetchType = fetchType;
        this.cascades = cascades;
        this.optional = optional;
        this.mappedBy = mappedBy;
        this.type = type;
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
     * Gets the target entity.
     * 
     * @return the targetEntity
     */
    public Class<?> getTargetEntity()
    {
        return targetEntity;
    }

    /**
     * @return the mapKeyJoinClass
     */
    public Class<?> getMapKeyJoinClass()
    {
        return mapKeyJoinClass;
    }

    /**
     * @param mapKeyJoinClass
     *            the mapKeyJoinClass to set
     */
    public void setMapKeyJoinClass(Class<?> mapKeyJoinClass)
    {
        this.mapKeyJoinClass = mapKeyJoinClass;
    }

    /**
     * Gets the property type.
     * 
     * @return the propertyType
     */
    public Class<?> getPropertyType()
    {
        return propertyType;
    }

    /**
     * Gets the fetch type.
     * 
     * @return the fetchType
     */
    public FetchType getFetchType()
    {
        return fetchType;
    }

    /**
     * Gets the cascades.
     * 
     * @return the cascades
     */
    public List<CascadeType> getCascades()
    {
        return cascades;
    }

    /**
     * Checks if is optional.
     * 
     * @return the optional
     */
    public boolean isOptional()
    {
        return optional;
    }

    /**
     * Gets the mapped by.
     * 
     * @return the mappedBy
     */
    public String getMappedBy()
    {
        return mappedBy;
    }

    /**
     * Gets the type.
     * 
     * @return the type
     */
    public Relation.ForeignKey getType()
    {
        return type;
    }

    /**
     * Gets the join column name.
     * 
     * @return the joinColumnName
     */
    public String getJoinColumnName()
    {

        if(joinColumnName == null && isJoinedByPrimaryKey)
        {
            EntityMetadata joinClassMetadata = KunderaMetadataManager.getEntityMetadata(targetEntity);
            joinColumnName  = ((AbstractAttribute)joinClassMetadata.getIdAttribute()).getJPAColumnName();
        }
        
        if(joinTableMetadata != null)
        {
            joinColumnName = joinTableMetadata.getJoinColumns() != null? joinTableMetadata.getJoinColumns().iterator().next():null;
        }
        
        if(isBiDirectional())
        {
            // Give precedence to join column name!
            if(biDirectionalField.isAnnotationPresent(JoinColumn.class))
            {
                joinColumnName = biDirectionalField.getAnnotation(JoinColumn.class).name();
            }
        }
        
        return joinColumnName !=null? joinColumnName:property.getName();
    }

    /**
     * Sets the join column name.
     * 
     * @param joinColumnName
     *            the joinColumnName to set
     */
    public void setJoinColumnName(String joinColumnName)
    {
        this.joinColumnName = joinColumnName;
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
     * @param isRelatedViaJoinTable
     *            the isRelatedViaJoinTable to set
     */
    public void setRelatedViaJoinTable(boolean isRelatedViaJoinTable)
    {
        this.isRelatedViaJoinTable = isRelatedViaJoinTable;
    }

    /**
     * Gets the join table metadata.
     * 
     * @return the joinTableMetadata
     */
    public JoinTableMetadata getJoinTableMetadata()
    {
        return joinTableMetadata;
    }

    /**
     * Sets the join table metadata.
     * 
     * @param joinTableMetadata
     *            the joinTableMetadata to set
     */
    public void setJoinTableMetadata(JoinTableMetadata joinTableMetadata)
    {
        this.joinTableMetadata = joinTableMetadata;
    }

    /**
     * Checks if is joined by primary key.
     * 
     * @return the isJoinedByPrimaryKey
     */
    public boolean isJoinedByPrimaryKey()
    {
        return isJoinedByPrimaryKey;
    }

    /**
     * Sets the joined by primary key.
     * 
     * @param isJoinedByPrimaryKey
     *            the isJoinedByPrimaryKey to set
     */
    public void setJoinedByPrimaryKey(boolean isJoinedByPrimaryKey)
    {
        this.isJoinedByPrimaryKey = isJoinedByPrimaryKey;
    }

    /**
     * Checks if is unary.
     * 
     * @return true, if is unary
     */
    public boolean isUnary()
    {
        return type.equals(Relation.ForeignKey.ONE_TO_ONE) || type.equals(Relation.ForeignKey.MANY_TO_ONE);
    }

    /**
     * Checks if is collection.
     * 
     * @return true, if is collection
     */
    public boolean isCollection()
    {
        return type.equals(Relation.ForeignKey.ONE_TO_MANY) || type.equals(Relation.ForeignKey.MANY_TO_MANY);
    }


    public boolean isBiDirectional()
    {
        return biDirectionalField != null;       
    }
    
    public Field getBiDirectionalField()
    {
        return biDirectionalField;
    }
    
    public void setBiDirectionalField(Class referencedClass)
    {
        Field[] fields = this.getTargetEntity().getDeclaredFields();
        Class<?> clazzz = null;
        for (Field field : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(field))
            {
                clazzz = field.getType();
                if (PropertyAccessorHelper.isCollection(clazzz))
                {
                    ParameterizedType type = (ParameterizedType) field.getGenericType();
                    Type[] types = type.getActualTypeArguments();
                    clazzz = (Class<?>) types[0];
                }
                else if (Map.class.isAssignableFrom(clazzz))
                {
                    ParameterizedType type = (ParameterizedType) field.getGenericType();
                    Type[] types = type.getActualTypeArguments();
                    clazzz = (Class<?>) types[1];
                }
                if (clazzz.equals(referencedClass))
                {
                    biDirectionalField = field;
                    break;
                }
            }
        }
 
    }
}