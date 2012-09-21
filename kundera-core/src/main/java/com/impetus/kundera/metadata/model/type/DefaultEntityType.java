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
package com.impetus.kundera.metadata.model.type;

import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Type;

/**
 * Default implementation of {@link EntityType}
 * 
 * <code> DefaultEmbeddableType</code> implements <code>EntityType</code>
 * interface, invokes constructor with PersistenceType.ENTITY. Default
 * implementation of {@link Type} interface is provided by {@link AbstractType}
 * 
 * @author vivek.mishra
 * @param <X>
 *            Entity generic java type.
 */

public class DefaultEntityType<X> extends AbstractIdentifiableType<X> implements EntityType<X>
{

    /**
     * Default constructor using fields.
     */
    public DefaultEntityType(Class<X> clazz, javax.persistence.metamodel.Type.PersistenceType persistenceType,
            AbstractIdentifiableType<? super X> superClazzType)
    {
        super(clazz, persistenceType, superClazzType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableType()
     */
    @Override
    public javax.persistence.metamodel.Bindable.BindableType getBindableType()
    {
        return BindableType.ENTITY_TYPE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Bindable#getBindableJavaType()
     */
    @Override
    public Class<X> getBindableJavaType()
    {
        return super.getJavaType();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.EntityType#getName()
     */
    @Override
    public String getName()
    {
        return getBindableJavaType().getSimpleName();
    }

}
