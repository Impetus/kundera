/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.commons.lang.NotImplementedException;

import com.impetus.kundera.cache.metadata.MetadataCacheManager;
import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * The Class QueryImpl.
 *
 * @author animesh.kumar
 */
public abstract class QueryImpl extends KunderaQuery implements Query
{

    /** The query. */
    protected String query;

    /**
     * Instantiates a new query impl.
     *
     * @param em
     *            the em
     * @param metadataCacheManager
     *            the metadata manager
     * @param query
     *            the query
     */
    public QueryImpl(EntityManagerImpl em, MetadataCacheManager metadataCacheManager, String query)
    {
        super(em, metadataCacheManager);
        this.query = query;
        parse();
    }

    /**
     * Gets the jPA query.
     *
     * @return the jPA query
     */
    public String getJPAQuery()
    {
        return query;
    }

    /**
     * Parses the.
     */
    private void parse()
    {
        KunderaQueryParser parser = new KunderaQueryParser(this, query);
        parser.parse();
        postParsingInit();
    }

    /* @see javax.persistence.Query#executeUpdate() */
    @Override
    public int executeUpdate()
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#getResultList() */
    @Override
    public List<?> getResultList()
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#getSingleResult() */
    @Override
    public Object getSingleResult()
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setFirstResult(int) */
    @Override
    public Query setFirstResult(int startPosition)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see
     * javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
     */
    @Override
    public Query setFlushMode(FlushModeType flushMode)
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setHint(java.lang.String, java.lang.Object) */
    @Override
    public Query setHint(String hintName, Object value)
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setMaxResults(int) */
    @Override
    public Query setMaxResults(int maxResult)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public Query setParameter(String name, Object value)
    {
        setParameter(name, value.toString());
        return this;
    }

    /* @see javax.persistence.Query#setParameter(int, java.lang.Object) */
    @Override
    public Query setParameter(int position, Object value)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Date value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Calendar value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Date,
     * javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Date value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Calendar,
     * javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Calendar value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getMaxResults()
     */
    @Override
    public int getMaxResults()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getFirstResult()
     */
    @Override
    public int getFirstResult()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getHints()
     */
    @Override
    public Map<String, Object> getHints()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter, java.lang.Object)
     */
    @Override
    public <T> Query setParameter(Parameter<T> paramParameter, T paramT)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter, java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Calendar> paramParameter, Calendar paramCalendar, TemporalType paramTemporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter, java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Date> paramParameter, Date paramDate, TemporalType paramTemporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameters()
     */
    @Override
    public Set<Parameter<?>> getParameters()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameter(java.lang.String)
     */
    @Override
    public Parameter<?> getParameter(String paramString)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameter(java.lang.String, java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameter(int)
     */
    @Override
    public Parameter<?> getParameter(int paramInt)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameter(int, java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(int paramInt, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#isBound(javax.persistence.Parameter)
     */
    @Override
    public boolean isBound(Parameter<?> paramParameter)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameterValue(javax.persistence.Parameter)
     */
    @Override
    public <T> T getParameterValue(Parameter<T> paramParameter)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameterValue(java.lang.String)
     */
    @Override
    public Object getParameterValue(String paramString)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getParameterValue(int)
     */
    @Override
    public Object getParameterValue(int paramInt)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getFlushMode()
     */
    @Override
    public FlushModeType getFlushMode()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#setLockMode(javax.persistence.LockModeType)
     */
    @Override
    public Query setLockMode(LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#getLockMode()
     */
    @Override
    public LockModeType getLockMode()
    {
        throw new NotImplementedException("TODO");
    }

    /* (non-Javadoc)
     * @see javax.persistence.Query#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }
}
