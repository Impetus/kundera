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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Tuple;
import javax.persistence.criteria.CollectionJoin;
import javax.persistence.criteria.CompoundSelection;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.ListJoin;
import javax.persistence.criteria.MapJoin;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.criteria.SetJoin;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.Metamodel;

import com.impetus.kundera.persistence.AbstractPredicate.ConditionalOperator;
import com.impetus.kundera.query.KunderaQuery.SortOrder;

/**
 * Implements criteria builder {@link CriteriaBuilder}.
 * 
 * @author vivek.mishra
 * 
 */
public class KunderaCriteriaBuilder implements CriteriaBuilder
{

    private EntityManagerFactory entityManagerFactory;

    KunderaCriteriaBuilder(EntityManagerFactory entityManagerFactory)
    {
        this.entityManagerFactory = entityManagerFactory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#abs(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<N> abs(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#all(javax.persistence.criteria
     * .Subquery)
     */
    @Override
    public <Y> Expression<Y> all(Subquery<Y> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#and(javax.persistence.criteria
     * .Predicate[])
     */
    @Override
    public Predicate and(Predicate... predicates)
    {
        return new ConjuctionPredicate(predicates);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#and(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate and(Expression<Boolean> arg0, Expression<Boolean> arg1)
    {
        if (arg0 != null && arg1 != null)
        {
           return new ConjuctionPredicate((Predicate)arg0, (Predicate)arg1);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#any(javax.persistence.criteria
     * .Subquery)
     */
    @Override
    public <Y> Expression<Y> any(Subquery<Y> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    
    CompoundSelection array( Class resultClazz, Selection<?>... arg0)
    {
        return new DefaultCompoundSelection(Arrays.asList(arg0), resultClazz);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#array(javax.persistence.criteria
     * .Selection<?>[])
     */
    @Override
    public CompoundSelection<Object[]> array(Selection<?>... arg0)
    {
        return new DefaultCompoundSelection<Object[]>(Arrays.asList(arg0), Object.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#asc(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Order asc(Expression<?> arg0)
    {
        return new QueryOrder(arg0, SortOrder.ASC);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#avg(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<Double> avg(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#between(javax.persistence.
     * criteria.Expression, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate between(Expression<? extends Y> arg0,
            Expression<? extends Y> arg1, Expression<? extends Y> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#between(javax.persistence.
     * criteria.Expression, java.lang.Comparable, java.lang.Comparable)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate between(Expression<? extends Y> arg0, Y arg1, Y arg2)
    {
        return new BetweenPredicate(arg0,arg1,arg2);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#coalesce()
     */
    @Override
    public <T> Coalesce<T> coalesce()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#coalesce(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y> Expression<Y> coalesce(Expression<? extends Y> arg0, Expression<? extends Y> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#coalesce(javax.persistence
     * .criteria.Expression, java.lang.Object)
     */
    @Override
    public <Y> Expression<Y> coalesce(Expression<? extends Y> arg0, Y arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#concat(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> concat(Expression<String> arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#concat(javax.persistence.criteria
     * .Expression, java.lang.String)
     */
    @Override
    public Expression<String> concat(Expression<String> arg0, String arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#concat(java.lang.String,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> concat(String arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#conjunction()
     */
    @Override
    public Predicate conjunction()
    {
        return new ConjuctionPredicate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#construct(java.lang.Class,
     * javax.persistence.criteria.Selection<?>[])
     */
    @Override
    public <Y> CompoundSelection<Y> construct(Class<Y> arg0, Selection<?>... arg1)
    {
        return new DefaultCompoundSelection<Y>(Arrays.asList(arg1), arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#count(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<Long> count(Expression<?> arg0)
    {
        // TODO Auto-generated method stub
        String arg1 = "Count("+arg0.getAlias()+")";
        return new AggregateExpression(arg0, arg1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#countDistinct(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<Long> countDistinct(Expression<?> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#createQuery()
     */
    @Override
    public CriteriaQuery<Object> createQuery()
    {
        return new KunderaCritieriaQuery<Object>(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#createQuery(java.lang.Class)
     */
    @Override
    public <T> CriteriaQuery<T> createQuery(Class<T> returnClazz)
    {
        return new KunderaCritieriaQuery<T>(this, returnClazz);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#createTupleQuery()
     */
    @Override
    public CriteriaQuery<Tuple> createTupleQuery()
    {
        return new KunderaCritieriaQuery<Tuple>(this, Tuple.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#currentDate()
     */
    @Override
    public Expression<Date> currentDate()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#currentTime()
     */
    @Override
    public Expression<Time> currentTime()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#currentTimestamp()
     */
    @Override
    public Expression<Timestamp> currentTimestamp()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#desc(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Order desc(Expression<?> arg0)
    {
        return new QueryOrder(arg0, SortOrder.DESC);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#diff(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> diff(Expression<? extends N> arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#diff(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public <N extends Number> Expression<N> diff(Expression<? extends N> arg0, N arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#diff(java.lang.Number,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> diff(N arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#disjunction()
     */
    @Override
    public Predicate disjunction()
    {
        return new DisjunctionPredicate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#equal(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate equal(Expression<?> lhs, Expression<?> rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.EQ);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#equal(javax.persistence.criteria
     * .Expression, java.lang.Object)
     */
    @Override
    public Predicate equal(Expression<?> lhs, Object rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.EQ);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#exists(javax.persistence.criteria
     * .Subquery)
     */
    @Override
    public Predicate exists(Subquery<?> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#function(java.lang.String,
     * java.lang.Class, javax.persistence.criteria.Expression<?>[])
     */
    @Override
    public <T> Expression<T> function(String arg0, Class<T> arg1, Expression<?>... arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#ge(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate ge(Expression<? extends Number> arg0, Expression<? extends Number> arg1)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#ge(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public Predicate ge(Expression<? extends Number> arg0, Number arg1)
    {
        // TODO Auto-generated method stub
        return new ComparisonPredicate(arg0, arg1, ConditionalOperator.GTE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#greaterThan(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate greaterThan(Expression<? extends Y> lhs,
            Expression<? extends Y> rhs)
    {

        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.GT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#greaterThan(javax.persistence
     * .criteria.Expression, java.lang.Comparable)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate greaterThan(Expression<? extends Y> lhs, Y rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.GT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#greaterThanOrEqualTo(javax
     * .persistence.criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate greaterThanOrEqualTo(Expression<? extends Y> lhs,
            Expression<? extends Y> rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.GTE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#greaterThanOrEqualTo(javax
     * .persistence.criteria.Expression, java.lang.Comparable)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate greaterThanOrEqualTo(Expression<? extends Y> lhs, Y rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.GTE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#greatest(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public <X extends Comparable<? super X>> Expression<X> greatest(Expression<X> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#gt(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate gt(Expression<? extends Number> arg0, Expression<? extends Number> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#gt(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public Predicate gt(Expression<? extends Number> lhs, Number rhs)
    {
        return new ComparisonPredicate(lhs, rhs, ConditionalOperator.GT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#in(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <T> In<T> in(Expression<? extends T> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isEmpty(javax.persistence.
     * criteria.Expression)
     */
    @Override
    public <C extends Collection<?>> Predicate isEmpty(Expression<C> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isFalse(javax.persistence.
     * criteria.Expression)
     */
    @Override
    public Predicate isFalse(Expression<Boolean> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isMember(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <E, C extends Collection<E>> Predicate isMember(Expression<E> arg0, Expression<C> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isMember(java.lang.Object,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <E, C extends Collection<E>> Predicate isMember(E arg0, Expression<C> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isNotEmpty(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public <C extends Collection<?>> Predicate isNotEmpty(Expression<C> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isNotMember(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <E, C extends Collection<E>> Predicate isNotMember(Expression<E> arg0, Expression<C> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isNotMember(java.lang.Object,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <E, C extends Collection<E>> Predicate isNotMember(E arg0, Expression<C> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isNotNull(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Predicate isNotNull(Expression<?> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isNull(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Predicate isNull(Expression<?> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#isTrue(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Predicate isTrue(Expression<Boolean> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#keys(java.util.Map)
     */
    @Override
    public <K, M extends Map<K, ?>> Expression<Set<K>> keys(M arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#le(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate le(Expression<? extends Number> arg0, Expression<? extends Number> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#le(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public Predicate le(Expression<? extends Number> arg0, Number arg1)
    {
        // TODO Auto-generated method stub
        return new ComparisonPredicate(arg0, arg1, ConditionalOperator.LTE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#least(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <X extends Comparable<? super X>> Expression<X> least(Expression<X> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#length(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<Integer> length(Expression<String> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#lessThan(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate lessThan(Expression<? extends Y> arg0,
            Expression<? extends Y> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#lessThan(javax.persistence
     * .criteria.Expression, java.lang.Comparable)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate lessThan(Expression<? extends Y> arg0, Y arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#lessThanOrEqualTo(javax.
     * persistence.criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate lessThanOrEqualTo(Expression<? extends Y> arg0,
            Expression<? extends Y> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#lessThanOrEqualTo(javax.
     * persistence.criteria.Expression, java.lang.Comparable)
     */
    @Override
    public <Y extends Comparable<? super Y>> Predicate lessThanOrEqualTo(Expression<? extends Y> arg0, Y arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate like(Expression<String> arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, java.lang.String)
     */
    @Override
    public Predicate like(Expression<String> arg0, String arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate like(Expression<String> arg0, Expression<String> arg1, Expression<Character> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression, char)
     */
    @Override
    public Predicate like(Expression<String> arg0, Expression<String> arg1, char arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, java.lang.String, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate like(Expression<String> arg0, String arg1, Expression<Character> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#like(javax.persistence.criteria
     * .Expression, java.lang.String, char)
     */
    @Override
    public Predicate like(Expression<String> arg0, String arg1, char arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#literal(java.lang.Object)
     */
    @Override
    public <T> Expression<T> literal(T arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#locate(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Integer> locate(Expression<String> arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#locate(javax.persistence.criteria
     * .Expression, java.lang.String)
     */
    @Override
    public Expression<Integer> locate(Expression<String> arg0, String arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#locate(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Integer> locate(Expression<String> arg0, Expression<String> arg1, Expression<Integer> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#locate(javax.persistence.criteria
     * .Expression, java.lang.String, int)
     */
    @Override
    public Expression<Integer> locate(Expression<String> arg0, String arg1, int arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#lower(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<String> lower(Expression<String> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#lt(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate lt(Expression<? extends Number> arg0, Expression<? extends Number> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#lt(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public Predicate lt(Expression<? extends Number> arg0, Number arg1)
    {
        return new ComparisonPredicate(arg0, arg1, ConditionalOperator.LT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#max(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<N> max(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#min(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<N> min(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#mod(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Integer> mod(Expression<Integer> arg0, Expression<Integer> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#mod(javax.persistence.criteria
     * .Expression, java.lang.Integer)
     */
    @Override
    public Expression<Integer> mod(Expression<Integer> arg0, Integer arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#mod(java.lang.Integer,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Integer> mod(Integer arg0, Expression<Integer> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#neg(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<N> neg(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#not(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Predicate not(Expression<Boolean> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notEqual(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate notEqual(Expression<?> arg0, Expression<?> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notEqual(javax.persistence
     * .criteria.Expression, java.lang.Object)
     */
    @Override
    public Predicate notEqual(Expression<?> arg0, Object arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, java.lang.String)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, String arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, Expression<String> arg1, Expression<Character> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, javax.persistence.criteria.Expression, char)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, Expression<String> arg1, char arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, java.lang.String,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, String arg1, Expression<Character> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#notLike(javax.persistence.
     * criteria.Expression, java.lang.String, char)
     */
    @Override
    public Predicate notLike(Expression<String> arg0, String arg1, char arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#nullLiteral(java.lang.Class)
     */
    @Override
    public <T> Expression<T> nullLiteral(Class<T> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#nullif(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <Y> Expression<Y> nullif(Expression<Y> arg0, Expression<?> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#nullif(javax.persistence.criteria
     * .Expression, java.lang.Object)
     */
    @Override
    public <Y> Expression<Y> nullif(Expression<Y> arg0, Y arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#or(javax.persistence.criteria
     * .Predicate[])
     */
    @Override
    public Predicate or(Predicate... predicates)
    {
        return new DisjunctionPredicate(predicates);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#or(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Predicate or(Expression<Boolean> arg0, Expression<Boolean> arg1)
    {
        // TODO Auto-generated method stub
        if (arg0 != null && arg1 != null)
        {
            if (arg0.getClass().isAssignableFrom(ComparisonPredicate.class) && arg1.getClass().isAssignableFrom(ComparisonPredicate.class))
            {
                return new DisjunctionPredicate((Predicate)arg0, (Predicate)arg1);
            }
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#parameter(java.lang.Class)
     */
    @Override
    public <T> ParameterExpression<T> parameter(Class<T> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#parameter(java.lang.Class,
     * java.lang.String)
     */
    @Override
    public <T> ParameterExpression<T> parameter(Class<T> arg0, String arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#prod(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> prod(Expression<? extends N> arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#prod(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public <N extends Number> Expression<N> prod(Expression<? extends N> arg0, N arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#prod(java.lang.Number,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> prod(N arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#quot(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Number> quot(Expression<? extends Number> arg0, Expression<? extends Number> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#quot(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public Expression<Number> quot(Expression<? extends Number> arg0, Number arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#quot(java.lang.Number,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<Number> quot(Number arg0, Expression<? extends Number> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#selectCase()
     */
    @Override
    public <R> Case<R> selectCase()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#selectCase(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public <C, R> SimpleCase<C, R> selectCase(Expression<? extends C> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#size(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <C extends Collection<?>> Expression<Integer> size(Expression<C> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#size(java.util.Collection)
     */
    @Override
    public <C extends Collection<?>> Expression<Integer> size(C arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#some(javax.persistence.criteria
     * .Subquery)
     */
    @Override
    public <Y> Expression<Y> some(Subquery<Y> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sqrt(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<Double> sqrt(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#substring(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> substring(Expression<String> arg0, Expression<Integer> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#substring(javax.persistence
     * .criteria.Expression, int)
     */
    @Override
    public Expression<String> substring(Expression<String> arg0, int arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#substring(javax.persistence
     * .criteria.Expression, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> substring(Expression<String> arg0, Expression<Integer> arg1, Expression<Integer> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#substring(javax.persistence
     * .criteria.Expression, int, int)
     */
    @Override
    public Expression<String> substring(Expression<String> arg0, int arg1, int arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sum(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public <N extends Number> Expression<N> sum(Expression<N> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sum(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> sum(Expression<? extends N> arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sum(javax.persistence.criteria
     * .Expression, java.lang.Number)
     */
    @Override
    public <N extends Number> Expression<N> sum(Expression<? extends N> arg0, N arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#sum(java.lang.Number,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public <N extends Number> Expression<N> sum(N arg0, Expression<? extends N> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sumAsDouble(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<Double> sumAsDouble(Expression<Float> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#sumAsLong(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<Long> sumAsLong(Expression<Integer> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toBigDecimal(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<BigDecimal> toBigDecimal(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toBigInteger(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<BigInteger> toBigInteger(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toDouble(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<Double> toDouble(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toFloat(javax.persistence.
     * criteria.Expression)
     */
    @Override
    public Expression<Float> toFloat(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toInteger(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<Integer> toInteger(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toLong(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<Long> toLong(Expression<? extends Number> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#toString(javax.persistence
     * .criteria.Expression)
     */
    @Override
    public Expression<String> toString(Expression<Character> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#trim(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<String> trim(Expression<String> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#trim(javax.persistence.criteria
     * .CriteriaBuilder.Trimspec, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> trim(Trimspec arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#trim(javax.persistence.criteria
     * .Expression, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> trim(Expression<Character> arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#trim(char,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> trim(char arg0, Expression<String> arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#trim(javax.persistence.criteria
     * .CriteriaBuilder.Trimspec, javax.persistence.criteria.Expression,
     * javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> trim(Trimspec arg0, Expression<Character> arg1, Expression<String> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#trim(javax.persistence.criteria
     * .CriteriaBuilder.Trimspec, char, javax.persistence.criteria.Expression)
     */
    @Override
    public Expression<String> trim(Trimspec arg0, char arg1, Expression<String> arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#tuple(javax.persistence.criteria
     * .Selection<?>[])
     */
    @Override
    public CompoundSelection<Tuple> tuple(Selection<?>... arg0)
    {
        return new DefaultCompoundSelection<Tuple>(Arrays.asList(arg0), Tuple.class);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.criteria.CriteriaBuilder#upper(javax.persistence.criteria
     * .Expression)
     */
    @Override
    public Expression<String> upper(Expression<String> arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.CriteriaBuilder#values(java.util.Map)
     */
    @Override
    public <V, M extends Map<?, V>> Expression<Collection<V>> values(M arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    Metamodel getMetaModel()
    {
        return this.entityManagerFactory.getMetamodel();
    }

    @Override
    public <T> CriteriaUpdate<T> createCriteriaUpdate(Class<T> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <T> CriteriaDelete<T> createCriteriaDelete(Class<T> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T, V extends T> Join<X, V> treat(Join<X, T> paramJoin, Class<V> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T, E extends T> CollectionJoin<X, E> treat(CollectionJoin<X, T> paramCollectionJoin, Class<E> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T, E extends T> SetJoin<X, E> treat(SetJoin<X, T> paramSetJoin, Class<E> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T, E extends T> ListJoin<X, E> treat(ListJoin<X, T> paramListJoin, Class<E> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, K, T, V extends T> MapJoin<X, K, V> treat(MapJoin<X, K, T> paramMapJoin, Class<V> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T extends X> Path<T> treat(Path<X> paramPath, Class<T> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

    @Override
    public <X, T extends X> Root<T> treat(Root<X> paramRoot, Class<T> paramClass)
    {
        //TODO: See https://github.com/impetus-opensource/Kundera/issues/457
        // Do nothing. Not yet implemented.
        return null;
    }

}
