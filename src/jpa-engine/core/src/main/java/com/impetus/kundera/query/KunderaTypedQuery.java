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
package com.impetus.kundera.query;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

/**
 * Implementation class for <code> javax.persitence.TypedQuery </code>
 * interface. More sort of a compile time type check wrapper on top of
 * {@link QueryImpl}.
 * 
 * @author vivek.mishra
 * 
 */
public class KunderaTypedQuery<X> implements TypedQuery<X>, com.impetus.kundera.query.Query
{
    /**
     * Query instance.
     */
    private Query query;

    /**
     * Constructor using query as parameter.
     * 
     * @param query
     *            query instance.
     */
    public KunderaTypedQuery(Query query)
    {
        this.query = query;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#executeUpdate()
     */
    @Override
    public int executeUpdate()
    {
        return query.executeUpdate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFirstResult()
     */
    @Override
    public int getFirstResult()
    {
        return query.getFirstResult();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFlushMode()
     */
    @Override
    public FlushModeType getFlushMode()
    {
        return query.getFlushMode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getHints()
     */
    @Override
    public Map<String, Object> getHints()
    {
        return query.getHints();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getLockMode()
     */
    @Override
    public LockModeType getLockMode()
    {
        return query.getLockMode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getMaxResults()
     */
    @Override
    public int getMaxResults()
    {

        return query.getMaxResults();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String)
     */
    @Override
    public Parameter<?> getParameter(String arg0)
    {

        return query.getParameter(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int)
     */
    @Override
    public Parameter<?> getParameter(int arg0)
    {
        return query.getParameter(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(String arg0, Class<T> arg1)
    {
        return query.getParameter(arg0, arg1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int, java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(int arg0, Class<T> arg1)
    {

        return query.getParameter(arg0, arg1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.Query#getParameterValue(javax.persistence.Parameter)
     */
    @Override
    public <T> T getParameterValue(Parameter<T> arg0)
    {

        return query.getParameterValue(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(java.lang.String)
     */
    @Override
    public Object getParameterValue(String arg0)
    {
        return query.getParameterValue(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(int)
     */
    @Override
    public Object getParameterValue(int arg0)
    {
        return query.getParameterValue(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameters()
     */
    @Override
    public Set<Parameter<?>> getParameters()
    {

        return query.getParameters();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#isBound(javax.persistence.Parameter)
     */
    @Override
    public boolean isBound(Parameter<?> arg0)
    {

        return query.isBound(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> arg0)
    {
        return query.unwrap(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#getResultList()
     */
    @Override
    public List<X> getResultList()
    {
        return (List<X>) query.getResultList();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#getSingleResult()
     */
    @Override
    public X getSingleResult()
    {
        return (X) query.getSingleResult();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setFirstResult(int)
     */
    @Override
    public TypedQuery<X> setFirstResult(int arg0)
    {

        query.setFirstResult(arg0);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.TypedQuery#setFlushMode(javax.persistence.FlushModeType
     * )
     */
    @Override
    public TypedQuery<X> setFlushMode(FlushModeType arg0)
    {
        query.setFlushMode(arg0);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setHint(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public TypedQuery<X> setHint(String arg0, Object arg1)
    {
        query.setHint(arg0, arg1);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.TypedQuery#setLockMode(javax.persistence.LockModeType)
     */
    @Override
    public TypedQuery<X> setLockMode(LockModeType arg0)
    {
        query.setLockMode(arg0);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setMaxResults(int)
     */
    @Override
    public TypedQuery<X> setMaxResults(int arg0)
    {
        query.setMaxResults(arg0);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.TypedQuery#setParameter(javax.persistence.Parameter,
     * java.lang.Object)
     */
    @Override
    public <T> TypedQuery<X> setParameter(Parameter<T> arg0, T arg1)
    {
        query.setParameter(arg0, arg1);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public TypedQuery<X> setParameter(String arg0, Object arg1)
    {
        query.setParameter(arg0, arg1);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(int, java.lang.Object)
     */
    @Override
    public TypedQuery<X> setParameter(int arg0, Object arg1)
    {
        query.setParameter(arg0, arg1);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.TypedQuery#setParameter(javax.persistence.Parameter,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(Parameter<Calendar> arg0, Calendar arg1, TemporalType arg2)
    {
        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.TypedQuery#setParameter(javax.persistence.Parameter,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(Parameter<Date> arg0, Date arg1, TemporalType arg2)
    {
        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(java.lang.String,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(String arg0, Calendar arg1, TemporalType arg2)
    {
        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(java.lang.String,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(String arg0, Date arg1, TemporalType arg2)
    {

        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(int, java.util.Calendar,
     * javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(int arg0, Calendar arg1, TemporalType arg2)
    {
        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TypedQuery#setParameter(int, java.util.Date,
     * javax.persistence.TemporalType)
     */
    @Override
    public TypedQuery<X> setParameter(int arg0, Date arg1, TemporalType arg2)
    {
        query.setParameter(arg0, arg1, arg2);
        return this;
    }

    @Override
    public void setFetchSize(Integer fetchsize)
    {
        ((com.impetus.kundera.query.Query)query).setFetchSize(fetchsize);
    }

    @Override
    public Integer getFetchSize()
    {
        return ((com.impetus.kundera.query.Query)query).getFetchSize();
    }

    @Override
    public void close()
    {
        ((com.impetus.kundera.query.Query)query).close();
        
    }

    @Override
    public Iterator<X> iterate()
    {
        return ((com.impetus.kundera.query.Query)query).iterate();
    }

    @Override
    public void applyTTL(int ttlInSeconds)
    {
        ((com.impetus.kundera.query.Query)query).applyTTL(ttlInSeconds);
    }

}
