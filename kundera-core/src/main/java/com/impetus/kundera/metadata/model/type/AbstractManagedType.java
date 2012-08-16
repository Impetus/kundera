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

import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.CollectionAttribute;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;

/**
 *  TODO::::: comments required.
 * @author vivek.mishra
 *
 */
public class AbstractManagedType<X> extends AbstractType<X> implements ManagedType<X>
{

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getAttributes()
     */
    @Override
    public Set<Attribute<? super X, ?>> getAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredAttributes()
     */
    @Override
    public Set<Attribute<X, ?>> getDeclaredAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getSingularAttribute(java.lang.String, java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<? super X, Y> getSingularAttribute(String paramString, Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredSingularAttribute(java.lang.String, java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<X, Y> getDeclaredSingularAttribute(String paramString, Class<Y> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getSingularAttributes()
     */
    @Override
    public Set<SingularAttribute<? super X, ?>> getSingularAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredSingularAttributes()
     */
    @Override
    public Set<SingularAttribute<X, ?>> getDeclaredSingularAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getCollection(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> CollectionAttribute<? super X, E> getCollection(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredCollection(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> CollectionAttribute<X, E> getDeclaredCollection(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getSet(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> SetAttribute<? super X, E> getSet(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredSet(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> SetAttribute<X, E> getDeclaredSet(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getList(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> ListAttribute<? super X, E> getList(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredList(java.lang.String, java.lang.Class)
     */
    @Override
    public <E> ListAttribute<X, E> getDeclaredList(String paramString, Class<E> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getMap(java.lang.String, java.lang.Class, java.lang.Class)
     */
    @Override
    public <K, V> MapAttribute<? super X, K, V> getMap(String paramString, Class<K> paramClass, Class<V> paramClass1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredMap(java.lang.String, java.lang.Class, java.lang.Class)
     */
    @Override
    public <K, V> MapAttribute<X, K, V> getDeclaredMap(String paramString, Class<K> paramClass, Class<V> paramClass1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getPluralAttributes()
     */
    @Override
    public Set<PluralAttribute<? super X, ?, ?>> getPluralAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredPluralAttributes()
     */
    @Override
    public Set<PluralAttribute<X, ?, ?>> getDeclaredPluralAttributes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getAttribute(java.lang.String)
     */
    @Override
    public Attribute<? super X, ?> getAttribute(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredAttribute(java.lang.String)
     */
    @Override
    public Attribute<X, ?> getDeclaredAttribute(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getSingularAttribute(java.lang.String)
     */
    @Override
    public SingularAttribute<? super X, ?> getSingularAttribute(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredSingularAttribute(java.lang.String)
     */
    @Override
    public SingularAttribute<X, ?> getDeclaredSingularAttribute(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getCollection(java.lang.String)
     */
    @Override
    public CollectionAttribute<? super X, ?> getCollection(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredCollection(java.lang.String)
     */
    @Override
    public CollectionAttribute<X, ?> getDeclaredCollection(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getSet(java.lang.String)
     */
    @Override
    public SetAttribute<? super X, ?> getSet(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredSet(java.lang.String)
     */
    @Override
    public SetAttribute<X, ?> getDeclaredSet(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getList(java.lang.String)
     */
    @Override
    public ListAttribute<? super X, ?> getList(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredList(java.lang.String)
     */
    @Override
    public ListAttribute<X, ?> getDeclaredList(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getMap(java.lang.String)
     */
    @Override
    public MapAttribute<? super X, ?, ?> getMap(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.metamodel.ManagedType#getDeclaredMap(java.lang.String)
     */
    @Override
    public MapAttribute<X, ?, ?> getDeclaredMap(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
