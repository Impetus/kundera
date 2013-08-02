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
import java.util.List;

import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Type;

/**
 * Implementation class for <code> {@link ListAttribute} </code> interface.
 * Offers metadata information implementation for collection attribute as per
 * jpa.
 * 
 * @author vivek.mishra
 * 
 * @param <X>
 *            managed type
 * @param <E>
 *            attribute type of list attribute.
 */
public class DefaultListAttribute<X, E> extends AbstractPluralAttribute<X, E, List<E>> implements ListAttribute<X, E>
{

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
     */
    public DefaultListAttribute(Type<E> attribType, String attribName,
            javax.persistence.metamodel.Attribute.PersistentAttributeType persistenceAttribType,
            ManagedType<X> managedType, Field member, Class<List<E>> clazz)
    {
        super(attribType, attribName, persistenceAttribType, managedType, member, clazz);
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
        return CollectionType.LIST;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.model.attributes.AbstractPluralAttribute
     * #getElementType()
     */
    @Override
    public Type<E> getElementType()
    {
        return attribType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Attribute#getJavaType()
     */
    @Override
    public Class<List<E>> getJavaType()
    {
        return super.getBoundJavaType();
    }
}
