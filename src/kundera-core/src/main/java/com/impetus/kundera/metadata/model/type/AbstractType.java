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

import javax.persistence.metamodel.Type;

/**
 * Implements <code> Type</code> interface of MetaModel API.
 * 
 * @param <X>
 *            the generic type
 * 
 * @author vivek.mishra
 */
public abstract class AbstractType<X> implements Type<X>
{

    /** The clazz type. */
    private Class<X> clazzType;

    /** The persistence type. */
    private PersistenceType persistenceType;

    /**
     * Instantiates a new default type.
     * 
     * @param clazz
     *            the clazz
     * @param persistenceType
     *            the persistence type
     */
    AbstractType(Class<X> clazz, PersistenceType persistenceType)
    {
        this.clazzType = clazz;
        this.persistenceType = persistenceType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Type#getPersistenceType()
     */
    @Override
    public javax.persistence.metamodel.Type.PersistenceType getPersistenceType()
    {
        return this.persistenceType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.Type#getJavaType()
     */
    @Override
    public Class<X> getJavaType()
    {
        return this.clazzType;
    }


}
