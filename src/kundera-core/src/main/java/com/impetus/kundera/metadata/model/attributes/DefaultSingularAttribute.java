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

import javax.persistence.Column;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for <code> {@link SetAttribute} </code> interface.
 * Offers metadata information implementation for collection attribute as per
 * jpa.
 * 
 * @author vivek.mishra
 * 
 * @param <X>
 *            managed type.
 * @param <T>
 *            attribute type.
 */

public class DefaultSingularAttribute<X, T> extends AbstractAttribute<X, T> implements SingularAttribute<X, T>
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(DefaultSingularAttribute.class);

    /** Attribute is an id? */
    private boolean isId;

    /**
     * @param attribName
     *            attribute name.
     * @param persistenceAttribType
     *            persistent attribute type.
     * @param member
     *            attribute's java member..
     * @param attribType
     *            attribute type.
     * @param managedType
     *            managed type.
     */
    public DefaultSingularAttribute(String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType, Field member,
            Type<T> attribType, ManagedType<X> managedType, boolean isId)
    {
        super(attribType, attribName, persistenceAttribType, managedType, member);
        this.isId = isId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#isCollection()
     */
    @Override
    public boolean isCollection()
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableType()
     */
    @Override
    public javax.persistence.metamodel.Bindable.BindableType getBindableType()
    {
        return BindableType.SINGULAR_ATTRIBUTE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.SingularAttribute#isId()
     */
    @Override
    public boolean isId()
    {
        return isId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.SingularAttribute#isVersion()
     */
    @Override
    public boolean isVersion()
    {
        log.info("Currently versioning is not supported in kundera, returning false as default");
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.SingularAttribute#isOptional()
     */
    @Override
    public boolean isOptional()
    {
        boolean isNullable = true;
        if (!isId())
        {
            Column anno = member.getAnnotation(Column.class);
            if (anno != null)
            {
                isNullable = anno.nullable();
            }
        }
        else
        {
            isNullable = false;
        }
        return isNullable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.SingularAttribute#getType()
     */
    @Override
    public Type<T> getType()
    {
        return attribType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.model.attributes.AbstractAttribute#getJavaType
     * ()
     */
    @Override
    public Class<T> getJavaType()
    {
        return attribType.getJavaType();
    }
}
