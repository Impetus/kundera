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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.PersistenceException;
import javax.persistence.PostLoad;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.DefaultEntityType;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;

/**
 * The Class QueryImpl.
 * 
 * @author vivek.mishra
 * @param <E>
 */
public abstract class QueryImpl<E> implements Query, com.impetus.kundera.query.Query
{

    /** The kundera query. */
    protected KunderaQuery kunderaQuery;

    /** The persistence delegeator. */
    protected PersistenceDelegator persistenceDelegeator;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(QueryImpl.class);

    private Set<Parameter<?>> parameters;

    private Map<String, Object> hints = new HashMap<String, Object>();

    /**
     * Default maximum result to fetch.
     */
    protected int maxResult = 100;

    private Integer fetchSize;

    
    /**
     * Instantiates a new query impl.
     * 
     * @param query
     *            the query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public QueryImpl(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        this.kunderaQuery = kunderaQuery;
        this.persistenceDelegeator = persistenceDelegator;
    }
    

    /**
     * Gets the jPA query.
     * 
     * @return the jPA query
     */
    public String getJPAQuery()
    {
        return kunderaQuery.getJPAQuery();
    }

    /**
     * Gets the kundera query.
     * 
     * @return the kunderaQuery
     */
    public KunderaQuery getKunderaQuery()
    {
        return kunderaQuery;
    }

    @Override
    public int executeUpdate()
    {
        return onExecuteUpdate();
    }

    @Override
    public List<?> getResultList()
    {
        if (log.isDebugEnabled())
            log.info("On getResultList() executing query: " + getJPAQuery());

        // as per JPA post event should happen before fetching data from database.
        
        handlePostEvent();
        
        List results = null;
        
        if (kunderaQuery.isDeleteUpdate())
        {
            executeUpdate();
        } else
        {
            results = fetch();
            assignReferenceToProxy(results);
        }
        
        return results != null ? results : new ArrayList();
    }


    protected List<Object> setRelationEntities(List enhanceEntities, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        List<Object> result = new ArrayList<Object>();
        if (enhanceEntities != null)
        {
            for (Object e : enhanceEntities)
            {
                if (!(e instanceof EnhanceEntity))
                {
                    e = new EnhanceEntity(e, PropertyAccessorHelper.getId(e, m), null);
                }
                EnhanceEntity ee = (EnhanceEntity) e;
                result.add(getReader().recursivelyFindEntities(ee.getEntity(), ee.getRelations(), m,
                        persistenceDelegeator, false));
            }
        }

        return result;
    }

    /**
     * Populate using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param result
     *            the result
     * @param columnsToSelect
     *            List of column names to be selected (rest should be ignored)
     * @return the list
     */
    protected List<Object> populateUsingLucene(EntityMetadata m, Client client, List<Object> result,
            String[] columnsToSelect)
    {
        String luceneQ = getLuceneQueryFromJPAQuery();
        Map<String, Object> searchFilter = client.getIndexManager().search(m.getEntityClazz(), luceneQ, Constants.INVALID,
                getMaxResults());
        String[] primaryKeys = searchFilter.values().toArray(new String[] {});
        Set<String> uniquePKs = new HashSet<String>(Arrays.asList(primaryKeys));

        if (kunderaQuery.isAliasOnly() || !m.getType().isSuperColumnFamilyMetadata())
        {

            result = (List<Object>) client.findAll(m.getEntityClazz(), columnsToSelect, uniquePKs.toArray());
        }
        else
        {
            return (List<Object>) persistenceDelegeator.find(m.getEntityClazz(), uniquePKs.toArray());
        }
        return result;
    }


    /**
     * Gets the lucene query from jpa query.
     * 
     * @return the lucene query from jpa query
     */
    protected String getLuceneQueryFromJPAQuery()
    {

        LuceneQueryBuilder queryBuilder = new LuceneQueryBuilder();
        EntityMetadata metadata = getKunderaQuery().getEntityMetadata();
        Metamodel metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        EntityType entity = metaModel.entity(metadata.getEntityClazz());

        for (Object object : kunderaQuery.getFilterClauseQueue())
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = filter.getProperty();
                String condition = filter.getCondition();
                String valueAsString = filter.getValue().toString();
                String fieldName = metadata.getFieldName(property);
                Class valueClazz = getValueType(entity, fieldName);
                
                queryBuilder.appendIndexName(metadata.getIndexName())
                        .appendPropertyName(getPropertyName(metadata, property))
                        .buildQuery(condition, valueAsString, valueClazz);
            } else
            {
                queryBuilder.buildQuery(object.toString(), object.toString(), String.class);
            }

        }

        queryBuilder.appendEntityName(kunderaQuery.getEntityClass().getCanonicalName().toLowerCase());
        return queryBuilder.getQuery();

    }

    private Class getValueType(EntityType entity, String fieldName)
    {
        Class valueClazz = null;
        if(fieldName != null)
        {
            valueClazz = ((AbstractAttribute) entity.getAttribute(fieldName)).getBindableJavaType();
        }
        return valueClazz;
    }


    /**
     * Populate entities, Method to populate data in case no relation exist!.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
    protected abstract List<Object> populateEntities(EntityMetadata m, Client client);

    protected abstract List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client);

    /**
     * Method returns entity reader.
     * 
     * @return entityReader entity reader.
     */
    protected abstract EntityReader getReader();

    /**
     * Method to be invoked on query.executeUpdate().
     * 
     * @return
     */
    protected abstract int onExecuteUpdate();

    /**
     * Returns entity metadata, in case of native query mapped class is present
     * within application metadata.
     * 
     * @return entityMetadata entity metadata.
     */
    protected EntityMetadata getEntityMetadata()
    {
        return kunderaQuery.getEntityMetadata();
    }

    protected int onUpdateDeleteEvent()
    {
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = fetch();
            onDeleteOrUpdate(result);
            return result != null ? result.size() : 0;
        }

        return 0;

    }
    
    /**
     * Performs delete or update based on query.
     * 
     * @param results
     *            list of objects to be merged/deleted.
     */
    protected void onDeleteOrUpdate(List results)
    {

        if (results != null)
        {
            if (!kunderaQuery.isUpdateClause())
            {
                // then case of delete
                for (Object result : results)
                {
                    persistenceDelegeator.remove(result);
                }
            }
            else
            {
                EntityMetadata entityMetadata = getEntityMetadata();
                for (Object result : results)
                {
                    for (UpdateClause c : kunderaQuery.getUpdateClauseQueue())
                    {
                        String columnName = c.getProperty();
                        try
                        {

                            DefaultEntityType entityType = (DefaultEntityType) KunderaMetadata.INSTANCE
                                    .getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit())
                                    .entity(entityMetadata.getEntityClazz());

                            // That will always be attribute name.

                            Attribute attribute = entityType.getAttribute(columnName);

                            // TODO : catch column name.
                            
                            if(c.getValue() instanceof String)
                            {
                                PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), c.getValue().toString());                                
                            }
                            else
                            {
                                PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), c.getValue());
                            }
                            persistenceDelegeator.merge(result);
                        }
                        catch (IllegalArgumentException iax)
                        {
                            log.error("Invalid column name: " + columnName + " for class : "
                                    + entityMetadata.getEntityClazz());
                            throw new QueryHandlerException("Error while executing query: " + iax);
                        }
                    }
                }
            }
        }
    }

    /************************* Methods from {@link Query} interface *******************************/

    /* @see javax.persistence.Query#getSingleResult() */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getSingleResult()
     */
    @Override
    public Object getSingleResult()
    {
        throw new UnsupportedOperationException("getSingleResult is unsupported by Kundera");
    }

    /* @see javax.persistence.Query#setFirstResult(int) */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setFirstResult(int)
     */
    @Override
    public Query setFirstResult(int startPosition)
    {
        throw new UnsupportedOperationException("setFirstResult is unsupported by Kundera");
    }

    /*
     * @see
     * javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
     */
    @Override
    public Query setFlushMode(FlushModeType flushMode)
    {
        throw new UnsupportedOperationException("setFlushMode is unsupported by Kundera");
    }

    /**
     * Sets hint name and value into hints map and returns instance of
     * {@link Query}
     */
    @Override
    public Query setHint(String hintName, Object value)
    {
        hints.put(hintName, value);
        return this;
    }

    /* @see javax.persistence.Query#setMaxResults(int) */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        this.maxResult = maxResult;
        return this;
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public Query setParameter(String name, Object value)
    {
        kunderaQuery.setParameter(name, value);
        return this;
    }

    /* @see javax.persistence.Query#setParameter(int, java.lang.Object) */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(int, java.lang.Object)
     */
    @Override
    public Query setParameter(int position, Object value)
    {
        kunderaQuery.setParameter(position, value);
        return this;
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Date, javax.persistence.TemporalType)
     */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Date value, TemporalType temporalType)
    {
        // Purpose of temporal type is to set value based on temporal type.
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Calendar value, TemporalType temporalType)
    {
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Date,
     * javax.persistence.TemporalType)
     */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(int, java.util.Date,
     * javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Date value, TemporalType temporalType)
    {
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Calendar,
     * javax.persistence.TemporalType)
     */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(int, java.util.Calendar,
     * javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Calendar value, TemporalType temporalType)
    {
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getMaxResults()
     */
    @Override
    public int getMaxResults()
    {
        return maxResult;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFirstResult()
     */
    @Override
    public int getFirstResult()
    {
        throw new UnsupportedOperationException("getFirstResult is unsupported by Kundera");
    }

    /**
     * Returns a {@link Map} containing query hints set by user
     */
    @Override
    public Map<String, Object> getHints()
    {
        return hints;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.lang.Object)
     */
    @Override
    public <T> Query setParameter(Parameter<T> paramParameter, T paramT)
    {
        if (!getParameters().contains(paramParameter))
        {
            throw new IllegalArgumentException("parameter does not correspond to a parameter of the query");
        }

        if (paramParameter.getName() != null)
        {
            kunderaQuery.setParameter(paramParameter.getName(), paramT);
        }
        else
        {
            kunderaQuery.setParameter(paramParameter.getPosition(), paramT);
        }

        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Calendar> paramParameter, Calendar paramCalendar, TemporalType paramTemporalType)
    {
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Date> paramParameter, Date paramDate, TemporalType paramTemporalType)
    {
        throw new UnsupportedOperationException("setParameter is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameters()
     */
    @Override
    public Set<Parameter<?>> getParameters()
    {
        if (parameters == null)
        {
            parameters = kunderaQuery.getParameters();
        }

        return parameters;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String)
     */
    @Override
    public Parameter<?> getParameter(String paramString)
    {
        onNativeCondition();
        getParameters();
        return getParameterByName(paramString);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(String paramString, Class<T> paramClass)
    {
        onNativeCondition();
        Parameter parameter = getParameterByName(paramString);
        return onTypeCheck(paramClass, parameter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int)
     */
    @Override
    public Parameter<?> getParameter(int paramInt)
    {
        onNativeCondition();
        getParameters();
        return getParameterByOrdinal(paramInt);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int, java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(int paramInt, Class<T> paramClass)
    {
        onNativeCondition();
        getParameters();
        Parameter parameter = getParameterByOrdinal(paramInt);
        return onTypeCheck(paramClass, parameter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#isBound(javax.persistence.Parameter)
     */
    @Override
    public boolean isBound(Parameter<?> paramParameter)
    {
        return kunderaQuery.isBound(paramParameter);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.Query#getParameterValue(javax.persistence.Parameter)
     */
    @Override
    public <T> T getParameterValue(Parameter<T> paramParameter)
    {
        Object value = kunderaQuery.getClauseValue(paramParameter);
        if (value == null)
        {
            throw new IllegalStateException("parameter has not been bound" + paramParameter);
        }
        return (T) value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(java.lang.String)
     */
    @Override
    public Object getParameterValue(String paramString)
    {

        return onParameterValue(":" + paramString);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(int)
     */
    @Override
    public Object getParameterValue(int paramInt)
    {
        return onParameterValue("?" + paramInt);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFlushMode()
     */
    @Override
    public FlushModeType getFlushMode()
    {
        throw new UnsupportedOperationException("getFlushMode is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setLockMode(javax.persistence.LockModeType)
     */
    @Override
    public Query setLockMode(LockModeType paramLockModeType)
    {
        throw new UnsupportedOperationException("setLockMode is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getLockMode()
     */
    @Override
    public LockModeType getLockMode()
    {
        throw new UnsupportedOperationException("getLockMode is unsupported by Kundera");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        try
        {
            return (T) this;
        }
        catch (ClassCastException ccex)
        {
            throw new PersistenceException("Provider does not support the call for class type:[" + paramClass + "]");
        }
    }

    /**
     * Returns specific parameter instance for given name
     * 
     * @param name
     *            parameter name.
     * 
     * @return parameter
     */
    private Parameter getParameterByName(String name)
    {
        if (getParameters() != null)
        {
            for (Parameter p : parameters)
            {
                if (name.equals(p.getName()))
                {
                    return p;
                }
            }
        }

        return null;
    }

    /**
     * Returns parameter by ordinal
     * 
     * @param position
     *            position
     * @return parameter instance.
     */
    private Parameter getParameterByOrdinal(Integer position)
    {
        for (Parameter p : parameters)
        {
            if (position.equals(p.getPosition()))
            {
                return p;
            }
        }

        return null;
    }

    /**
     * Method to handle get/set Parameter supplied for native query.
     */
    private void onNativeCondition()
    {
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        if (appMetadata.isNative(getJPAQuery()))
        {
            throw new IllegalStateException(
                    "invoked on a native query when the implementation does not support this use");
        }
    }

    /**
     * Validated parameter's class with input paramClass. Returns back parameter
     * if it matches, else throws an {@link IllegalArgumentException}.
     * 
     * @param <T>
     *            type of class.
     * @param paramClass
     *            expected class type.
     * @param parameter
     *            parameter
     * @return parameter if it matches, else throws an
     *         {@link IllegalArgumentException}.
     */
    private <T> Parameter<T> onTypeCheck(Class<T> paramClass, Parameter<T> parameter)
    {
        if (parameter != null && parameter.getParameterType() != null && parameter.getParameterType().equals(paramClass))
        {
            return parameter;
        }
        throw new IllegalArgumentException(
                "The parameter of the specified name does not exist or is not assignable to the type");
    }

    /**
     * Returns parameter value.
     * 
     * @param paramString
     *            parameter as string.
     * 
     * @return value of parameter.
     */
    private Object onParameterValue(String paramString)
    {
        Object value = kunderaQuery.getClauseValue(paramString);
        if (value == null)
        {
            throw new IllegalStateException("parameter has not been bound" + paramString);
        }
        return value;
    }

    protected String[] getColumns(final String[] columns, final EntityMetadata m)
    {
        List<String> columnAsList = new ArrayList<String>();
        if (columns != null && columns.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());
            for (int i = 1; i < columns.length; i++)
            {
                if (columns[i] != null)
                {
                    Attribute col = entity.getAttribute(columns[i]);
                    if (col == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + columns);
                    }
                    columnAsList.add(((AbstractAttribute) col).getJPAColumnName());
                }
            }
        }
        return columnAsList.toArray(new String[] {});
    }

    public void setFetchSize(Integer fetchsize)
    {
        this.fetchSize = fetchsize;
    }

    public Integer getFetchSize()
    {
        return this.fetchSize;
    }

    public abstract void close();
    
    public abstract <E> Iterator<E> iterate();


    /**
     * Handle post even callbacks.
     * 
     */
    protected void handlePostEvent()
    {
        EntityMetadata metadata = getEntityMetadata();
        if(!kunderaQuery.isDeleteUpdate())
        {
            persistenceDelegeator.getEventDispatcher().fireEventListeners(metadata, null, PostLoad.class);
        }
    }

    /**
     *  Returns collection of fetched entities.
     *  
     * @return 
     */
    protected List fetch()
    {
        EntityMetadata metadata = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(metadata);
        List results = isRelational(metadata) ? recursivelyPopulateEntities(metadata, client):populateEntities(metadata, client);
        return results;
    }

    /**
     * Returns true, if associated entity holds relational references(e.g. @OneToMany etc.) else false.
     * 
     * @param m entity metadata
     * 
     * @return  true, if holds relation else false
     */
    private boolean isRelational(EntityMetadata m)
    {
        // if related via join table OR contains relations.
        return m.isRelationViaJoinTable() || (m.getRelationNames() != null && (!m.getRelationNames().isEmpty()));
    }

    
    /**
     * If returned collection of object holds a reference to 
     * @param results
     */
    private void assignReferenceToProxy(List results)
    {
        if(results != null)
        {
            for(Object obj : results)
            {
                KunderaMetadata.INSTANCE.getCoreMetadata().getLazyInitializerFactory().setProxyOwners(getEntityMetadata(), obj);
            }
        }
    }

    private String getPropertyName(final EntityMetadata metadata, final String property)
    {
        if (MetadataUtils.getEnclosingEmbeddedFieldName(metadata, property, true) != null)
        {
            return property.substring(property.indexOf(".") + 1,
                    property.length());
        }
        
        return property; 
                
    }
}
