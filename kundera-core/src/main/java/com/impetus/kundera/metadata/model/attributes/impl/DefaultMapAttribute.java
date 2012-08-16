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
package com.impetus.kundera.metadata.model.attributes.impl;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.Type;

import com.impetus.kundera.metadata.model.attributes.AbstractPluralAttribute;


/**
 * TODO::::: comments required.
 * 
 * @author vivek.mishra
 * 
 * @param <X>
 * @param <E>
 */
public class DefaultMapAttribute<X, K, V> extends AbstractPluralAttribute<X, V, Map<K, V>> implements
        MapAttribute<X, K, V>
{

    /**
     * @param attribType
     * @param attribName
     * @param persistenceAttribType
     * @param managedType
     * @param member
     */
    public DefaultMapAttribute(Type<V> attribType, String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType,
            ManagedType<X> managedType, Field member)
    {
        super(attribType, attribName, persistenceAttribType, managedType, member);
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.MapAttribute#getKeyJavaType()
     */
    @Override
    public Class<K> getKeyJavaType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.MapAttribute#getKeyType()
     */
    @Override
    public Type<K> getKeyType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.model.attributes.AbstractPluralAttribute
     * #getCollectionType()
     */
    @Override
    public javax.persistence.metamodel.PluralAttribute.CollectionType getCollectionType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.model.attributes.AbstractPluralAttribute
     * #getElementType()
     */
    @Override
    public Type<V> getElementType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.metadata.model.attributes.AbstractAttribute#
     * getBindableType()
     */
    @Override
    public javax.persistence.metamodel.Bindable.BindableType getBindableType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.model.attributes.AbstractAttribute#isCollection
     * ()
     */
    @Override
    public boolean isCollection()
    {
        // TODO Auto-generated method stub
        return false;
    }
}
