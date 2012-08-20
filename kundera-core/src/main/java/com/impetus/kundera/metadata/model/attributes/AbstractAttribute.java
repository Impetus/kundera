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

import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Type;

/**
 * Abstract class for to provide generalization, abstraction to <code>Type</code> hierarchy.
 *
 * @param <X> the generic mananged entitytype
 * @param <T> the generic attribute type
 * @author vivek.mishra
 */
public abstract class AbstractAttribute<X, T>
{
    
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

    /**
     * Instantiates a new abstract attribute.
     *
     * @param attribType the attrib type
     * @param attribName the attrib name
     * @param persistenceAttribType the persistence attrib type
     * @param managedType the managed type
     * @param member the member
     */
    public AbstractAttribute(Type<T> attribType, String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType,
            ManagedType<X> managedType, Field member)
    {

        this.attribType = attribType;
        this.attribName = attribName;
        this.persistenceAttribType = persistenceAttribType;
        this.managedType = managedType;
        this.member = member;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableType()
     */

    /**
     * Gets the bindable type.
     *
     * @return the bindable type
     */
    public abstract javax.persistence.metamodel.Bindable.BindableType getBindableType();

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#isCollection()
     */

    /**
     * Checks if is collection.
     *
     * @return true, if is collection
     */
    public abstract boolean isCollection();

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableJavaType()
     */

    /**
     * Gets the bindable java type.
     *
     * @return the bindable java type
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

    /**
     * Gets the name.
     *
     * @return the name
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

    /**
     * Gets the persistent attribute type.
     *
     * @return the persistent attribute type
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

    /**
     * Gets the declaring type.
     *
     * @return the declaring type
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

    /**
     * Gets the java member.
     *
     * @return the java member
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

    /**
     * Checks if is association.
     *
     * @return true, if is association
     */
    public boolean isAssociation()
    {
        return persistenceAttribType.equals(PersistentAttributeType.MANY_TO_MANY)
                || persistenceAttribType.equals(PersistentAttributeType.MANY_TO_ONE)
                || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_MANY)
                || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_ONE);
    }

}
