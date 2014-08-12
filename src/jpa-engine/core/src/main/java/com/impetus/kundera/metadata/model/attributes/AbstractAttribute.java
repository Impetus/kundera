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
package com.impetus.kundera.metadata.model.attributes;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.util.Date;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.JoinColumn;
import javax.persistence.Temporal;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.annotation.DefaultFieldAnnotationProcessor;
import com.impetus.kundera.metadata.model.annotation.FieldAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;

/**
 * Abstract class for to provide generalisation, abstraction to
 * <code>Type</code> hierarchy.
 * 
 * @param <X>
 *            the generic mananged entitytype
 * @param <T>
 *            the generic attribute type
 * @author vivek.mishra
 */
public abstract class AbstractAttribute<X, T>
{

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(AbstractAttribute.class);

    /** The attrib type. */
    protected Type<T> attribType;

    /** The attrib name. */
    private String attribName;

    /** The persistence attrib type. */
    private PersistentAttributeType persistenceAttribType;

    /** The managed type. */
    private ManagedType<X> managedType;

    /** The member. */
    protected Field member;

    /** Column name */
    private String columnName;

    /** Name of Table, to which this attribute belongs to */
    private String tableName;

    private FieldAnnotationProcessor fieldAnnotationProcessor;

    /**
     * Instantiates a new abstract attribute.
     * 
     * @param attribType
     *            the attrib type
     * @param attribName
     *            the attrib name
     * @param persistenceAttribType
     *            the persistence attrib type
     * @param managedType
     *            the managed type
     * @param member
     *            the member
     */
    AbstractAttribute(Type<T> attribType, String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType,
            ManagedType<X> managedType, Field member)
    {

        this.attribType = attribType;
        this.attribName = attribName;
        this.persistenceAttribType = persistenceAttribType;
        this.managedType = managedType;
        this.member = member;
        this.columnName = getValidJPAColumnName();
        this.fieldAnnotationProcessor = new DefaultFieldAnnotationProcessor(member);
        this.fieldAnnotationProcessor.validateFieldAnnotation(
                fieldAnnotationProcessor.getAnnotation(Column.class.getName()), (Field) member, this.managedType);
        this.tableName = getTableName();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableType()
     */
    public abstract javax.persistence.metamodel.Bindable.BindableType getBindableType();

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#isCollection()
     */
    public abstract boolean isCollection();

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableJavaType()
     */
    public Class<T> getBindableJavaType()
    {
        return attribType.getJavaType();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getName()
     */
    public String getName()
    {
        return attribName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getPersistentAttributeType()
     */

    public javax.persistence.metamodel.Attribute.PersistentAttributeType getPersistentAttributeType()
    {
        return persistenceAttribType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getDeclaringType()
     */
    public ManagedType<X> getDeclaringType()
    {
        return managedType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getJavaMember()
     */
    public Member getJavaMember()
    {
        return member;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#isAssociation()
     */
    public boolean isAssociation()
    {
        return persistenceAttribType.equals(PersistentAttributeType.MANY_TO_MANY)
                || persistenceAttribType.equals(PersistentAttributeType.MANY_TO_ONE)
                || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_MANY)
                || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_ONE);
    }

    /**
     * Returns assigned jpa column name.
     * 
     * @return column name jpa column name.
     */
    public String getJPAColumnName()
    {
        // In case of Attribute override.
        Column column = ((AbstractManagedType) this.managedType).getAttributeBinding(member);
        if (column != null)
        {
            columnName = column.name();
        }

        return columnName;
    }

    /**
     * Returns assigned table name.
     * 
     * @return table name.
     */
    public String getTableName()
    {
        return ((DefaultFieldAnnotationProcessor) fieldAnnotationProcessor).getTableNameOfColumn();
    }

    /**
     * Gets the valid jpa column name.
     * 
     * @param entity
     *            the entity
     * @param f
     *            the f
     * @return the valid jpa column name
     */
    private final String getValidJPAColumnName()
    {

        String name = null;
        if (member.isAnnotationPresent(Column.class))
        {
            Column c = member.getAnnotation(Column.class);
            if (!c.name().isEmpty())
            {
                name = c.name();
            }

        }
        if (member.isAnnotationPresent(Temporal.class))
        {
            if (!member.getType().equals(Date.class))
            {
                log.error("@Temporal must map to java.util.Date for @Entity(" + managedType.getJavaType() + "."
                        + member.getName() + ")");
                return name;
            }
        }
        else if (member.isAnnotationPresent(JoinColumn.class))
        {
            JoinColumn c = member.getAnnotation(JoinColumn.class);
            if (!c.name().isEmpty())
            {
                name = c.name();
            }
        }
        else if (member.isAnnotationPresent(CollectionTable.class))
        {
            CollectionTable c = member.getAnnotation(CollectionTable.class);
            if (!c.name().isEmpty())
            {
                name = c.name();
            }
        }

        return name == null ? getName() : name;
    }

    public FieldAnnotationProcessor getFieldAnnotation()
    {
        return fieldAnnotationProcessor;
    }
    
    public void setColumnName(final String columnName)
    {
        this.columnName = columnName;
    }
}
