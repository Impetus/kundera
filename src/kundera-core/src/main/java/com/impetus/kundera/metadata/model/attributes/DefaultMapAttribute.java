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
import java.util.Map;

import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.Type;

/**
 * Implementation class for <code> {@link MapAttribute} </code> interface.
 * Offers metadata information implementation for collection attribute as per
 * jpa.
 * 
 * @author vivek.mishra
 * 
 * @param <X>
 *            managed type
 * @param <E>
 *            attribute type present in map.
 */
public class DefaultMapAttribute<X, K, V> extends AbstractPluralAttribute<X, V, Map<K, V>> implements
        MapAttribute<X, K, V>
{

    private Type<K> keyType;

    /**
     * Constructor using fields.
     * 
     * @param attribType
     *            attribute type
     * @param attribName
     *            attribute name
     * @param persistenceAttribType
     *            persistent attribute type.
     * @param managedType
     *            managed type
     * @param member
     *            attribute's java member.
     * @param key
     *            type attribute of key type.
     */
    public DefaultMapAttribute(Type<V> attribType, String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType,
            ManagedType<X> managedType, Field member, Class<Map<K, V>> clazz, Type<K> keyType)
    {
        super(attribType, attribName, persistenceAttribType, managedType, member, clazz);
        this.keyType = keyType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.MapAttribute#getKeyJavaType()
     */
    @Override
    public Class<K> getKeyJavaType()
    {
        return this.keyType.getJavaType();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.MapAttribute#getKeyType()
     */
    @Override
    public Type<K> getKeyType()
    {
        return this.keyType;
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
        return CollectionType.MAP;
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
        return attribType;
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
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getJavaType()
     */
    @Override
    public Class<Map<K, V>> getJavaType()
    {
        return super.getBoundJavaType();
    }

}
