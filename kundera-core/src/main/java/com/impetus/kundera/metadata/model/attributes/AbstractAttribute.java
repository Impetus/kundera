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
 *  TODO::::: comments required.
 * @author vivek.mishra
 *
 * @param <X>
 * @param <T>
 */
public abstract class AbstractAttribute<X,T>
{
    protected Type<T> attribType;
    private String attribName;
    private PersistentAttributeType persistenceAttribType;
    private ManagedType<X> managedType;
    protected Field member;
    
    
    /**
     * @param attribType
     * @param attribName
     * @param persistenceAttribType
     * @param managedType
     * @param member
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

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Bindable#getBindableType()
     */
    
    public abstract javax.persistence.metamodel.Bindable.BindableType getBindableType();
    
    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#isCollection()
     */
    
    public abstract boolean isCollection();
    

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Bindable#getBindableJavaType()
     */
    
    public Class<T> getBindableJavaType()
    {
        return attribType.getJavaType();
    }
    

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#getName()
     */
    
    public String getName()
    {
        return attribName;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#getPersistentAttributeType()
     */
    
    public javax.persistence.metamodel.Attribute.PersistentAttributeType getPersistentAttributeType()
    {
        return persistenceAttribType;
    }
    

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#getDeclaringType()
     */
    
    public ManagedType<X> getDeclaringType()
    {
        return managedType;
    }
    

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#getJavaMember()
     */
    
    public Member getJavaMember()
    {
        return member;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.Attribute#isAssociation()
     */
    
    public boolean isAssociation()
    {
        return persistenceAttribType.equals(PersistentAttributeType.MANY_TO_MANY)
        || persistenceAttribType.equals(PersistentAttributeType.MANY_TO_ONE)
        || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_MANY)
        || persistenceAttribType.equals(PersistentAttributeType.ONE_TO_ONE);
    }

  

}
