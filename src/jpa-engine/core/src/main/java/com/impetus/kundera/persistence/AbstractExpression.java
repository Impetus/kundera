/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.Collection;
import java.util.List;

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Selection;

/**
 * The Class AbstractExpression.
 * 
 * @param <T>
 *            the generic type
 * @author: karthikp.manchala
 */
public class AbstractExpression<T> implements Expression<T>
{

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Selection#alias(java.lang.String)
     */
    @Override
    public Selection<T> alias(String name)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Selection#isCompoundSelection()
     */
    @Override
    public boolean isCompoundSelection()
    {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Selection#getCompoundSelectionItems()
     */
    @Override
    public List<Selection<?>> getCompoundSelectionItems()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TupleElement#getJavaType()
     */
    @Override
    public Class<? extends T> getJavaType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TupleElement#getAlias()
     */
    @Override
    public String getAlias()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#isNull()
     */
    @Override
    public Predicate isNull()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#isNotNull()
     */
    @Override
    public Predicate isNotNull()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#in(java.lang.Object[])
     */
    @Override
    public Predicate in(Object... values)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#in(javax.persistence.criteria.
     * Expression[])
     */
    @Override
    public Predicate in(Expression<?>... values)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#in(java.util.Collection)
     */
    @Override
    public Predicate in(Collection<?> values)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#in(javax.persistence.criteria.
     * Expression)
     */
    @Override
    public Predicate in(Expression<Collection<?>> values)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Expression#as(java.lang.Class)
     */
    @Override
    public <X> Expression<X> as(Class<X> type)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
