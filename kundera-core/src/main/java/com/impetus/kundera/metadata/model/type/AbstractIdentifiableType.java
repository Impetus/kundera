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

import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;

/**
 *  TODO::::: comments required.
 * @author vivek.mishra
 *
 */
public class AbstractIdentifiableType<X> extends AbstractManagedType<X> implements IdentifiableType<X>
{

    /**
     * @param clazz
     * @param persistenceType
     * @param superClazzType
     * @param declaredSingluarAttribs
     * @param declaredPluralAttributes
     */
    public AbstractIdentifiableType(Class<X> clazz, javax.persistence.metamodel.Type.PersistenceType persistenceType,
            ManagedType<? super X> superClazzType, Map<String, SingularAttribute<X, ?>> declaredSingluarAttribs,
            Map<String, PluralAttribute<X, ?, ?>> declaredPluralAttributes)
    {
        super(clazz, persistenceType, superClazzType, declaredSingluarAttribs, declaredPluralAttributes);
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getId(java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<? super X, Y> getId(Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getDeclaredId(java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<X, Y> getDeclaredId(Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getVersion(java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<? super X, Y> getVersion(Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getDeclaredVersion(java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<X, Y> getDeclaredVersion(Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getSupertype()
     */
    @Override
    public IdentifiableType<? super X> getSupertype()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#hasSingleIdAttribute()
     */
    @Override
    public boolean hasSingleIdAttribute()
    {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#hasVersionAttribute()
     */
    @Override
    public boolean hasVersionAttribute()
    {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getIdClassAttributes()
     */
    @Override
    public Set<SingularAttribute<? super X, ?>> getIdClassAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.IdentifiableType#getIdType()
     */
    @Override
    public Type<?> getIdType()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
