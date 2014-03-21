/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
import java.util.Collections;
import java.util.List;

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Selection;

/**
 * Abstract super class for {@link Predicate}
 *  
 * @author vivek.mishra
 *
 */
abstract class AbstractPredicate implements Predicate
{



    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#getOperator()
     */
    @Override
    public BooleanOperator getOperator()
    {
        return BooleanOperator.AND;
    }


    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#isNull()
     */
    @Override
    public Predicate isNull()
    {
        throw new UnsupportedOperationException("Method isNull() not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#isNotNull()
     */
    @Override
    public Predicate isNotNull()
    {
        throw new UnsupportedOperationException("Method isNotNull() not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#in(java.lang.Object[])
     */
    @Override
    public Predicate in(Object... paramArrayOfObject)
    {
        throw new UnsupportedOperationException("Method in(Object... paramArrayOfObject) not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#in(javax.persistence.criteria.Expression<?>[])
     */
    @Override
    public Predicate in(Expression<?>... paramArrayOfExpression)
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Method in(Expression<?>... paramArrayOfExpression) not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#in(java.util.Collection)
     */
    @Override
    public Predicate in(Collection<?> paramCollection)
    {
        throw new UnsupportedOperationException("Method in(Collection<?> paramCollection) not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#in(javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate in(Expression<Collection<?>> paramExpression)
    {
        throw new UnsupportedOperationException("Method in(Expression<Collection<?>> paramExpression) not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Expression#as(java.lang.Class)
     */
    @Override
    public <X> Expression<X> as(Class<X> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Selection#alias(java.lang.String)
     */
    @Override
    public Selection<Boolean> alias(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Selection#isCompoundSelection()
     */
    @Override
    public boolean isCompoundSelection()
    {
        return false;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Selection#getCompoundSelectionItems()
     */
    @Override
    public List<Selection<?>> getCompoundSelectionItems()
    {
        return Collections.emptyList();
    }

    /* (non-Javadoc)
     * @see javax.persistence.TupleElement#getJavaType()
     */
    @Override
    public Class<? extends Boolean> getJavaType()
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.TupleElement#getAlias()
     */
    @Override
    public String getAlias()
    {
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#isNegated()
     */
    @Override
    public boolean isNegated()
    {
        throw new UnsupportedOperationException("Method isNegated() not yet supported");
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#not()
     */
    @Override
    public Predicate not()
    {
        throw new UnsupportedOperationException("Method not() not yet supported");
    }

    
    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#getExpressions()
     */
    @Override
    public List<Expression<Boolean>> getExpressions()
    {
        return java.util.Collections.emptyList();
    }


    public static enum ConditionalOperator
    {
        EQ, NEQ, LT, LTE, GT, GTE, BTW;
    }
}
