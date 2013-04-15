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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.index.DocumentIndexer;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
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
 */
public abstract class QueryImpl implements Query
{

    /** The query. */
    protected String query;

    /** The kundera query. */
    protected KunderaQuery kunderaQuery;

    /** The persistence delegeator. */
    protected PersistenceDelegator persistenceDelegeator;

    /** The log. */
    private static Log log = LogFactory.getLog(QueryImpl.class);

    private Set<Parameter<?>> parameters;

    private Map<String, Object> hints = new HashMap<String, Object>();

    /**
     * Default maximum result to fetch.
     */
    protected int maxResult = 100;

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
    public QueryImpl(String query, PersistenceDelegator persistenceDelegator)
    {

        this.query = query;
        this.persistenceDelegeator = persistenceDelegator;
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
     * Gets the kundera query.
     * 
     * @return the kunderaQuery
     */
    public KunderaQuery getKunderaQuery()
    {
        return kunderaQuery;
    }

    /**
     * Sets the kundera query.
     * 
     * @param kunderaQuery
     *            the kunderaQuery to set
     */
    public void setKunderaQuery(KunderaQuery kunderaQuery)
    {
        this.kunderaQuery = kunderaQuery;
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
            log.info("On getResultList() executing query: " + query);
        List results = new ArrayList();

        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);

        if (!m.isRelationViaJoinTable() && (m.getRelationNames() == null || (m.getRelationNames().isEmpty())))
        {
            results = populateEntities(m, client);
        }
        else
        {
            results = recursivelyPopulateEntities(m, client);
        }

        // If intended for delete/update.
        if (kunderaQuery.isDeleteUpdate())
        {
            onDeleteOrUpdate(results);
        }
        return results != null ? results : new ArrayList();
    }

    /**
     * Gets the persistence delegeator.
     * 
     * @return the persistenceDelegeator
     */
    public PersistenceDelegator getPersistenceDelegeator()
    {
        return persistenceDelegeator;
    }

    /**
     * Sets the persistence delegeator.
     * 
     * @param persistenceDelegeator
     *            the persistenceDelegeator to set
     */
    public void setPersistenceDelegeator(PersistenceDelegator persistenceDelegeator)
    {
        this.persistenceDelegeator = persistenceDelegeator;
    }

    protected List<Object> setRelationEntities(List enhanceEntities, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        List<Object> result = null;
        if (enhanceEntities != null)
        {
            for (Object e : enhanceEntities)
            {
                if (result == null)
                {
                    result = new ArrayList<Object>(enhanceEntities.size());
                }

                if (!(e instanceof EnhanceEntity))
                {
                    e = new EnhanceEntity(e, PropertyAccessorHelper.getId(e, m), null);
                }

                EnhanceEntity ee = (EnhanceEntity) e;

                result.add(getReader().recursivelyFindEntities(ee.getEntity(), ee.getRelations(), m,
                        persistenceDelegeator));
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
     * @return the list
     */
    protected List<Object> populateUsingLucene(EntityMetadata m, Client client, List<Object> result)
    {
        String luceneQ = getLuceneQueryFromJPAQuery();
        Map<String, Object> searchFilter = client.getIndexManager().search(luceneQ, Constants.INVALID,
                Constants.INVALID);
        if (kunderaQuery.isAliasOnly() || !m.getType().isSuperColumnFamilyMetadata())
        {
            String[] primaryKeys = searchFilter.values().toArray(new String[] {});
            Set<String> uniquePKs = new HashSet<String>(Arrays.asList(primaryKeys));

            // result = (List<Object>)
            // persistenceDelegeator.find(m.getEntityClazz(),
            // uniquePKs.toArray());
            result = (List<Object>) client.findAll(m.getEntityClazz(), uniquePKs.toArray());

        }
        else
        {
            return (List<Object>) persistenceDelegeator.find(m.getEntityClazz(), searchFilter);
        }
        return result;
    }

    /**
     * Populate relations.
     * 
     * @param relations
     *            the relations
     * @param o
     *            the o
     * @return the map
     */
    protected Map<String, Object> populateRelations(List<String> relations, Object[] o)
    {
        Map<String, Object> relationVal = new HashMap<String, Object>(relations.size());
        int counter = 1;
        for (String r : relations)
        {
            relationVal.put(r, o[counter++]);
        }
        return relationVal;
    }

    /**
     * Gets the lucene query from jpa query.
     * 
     * @return the lucene query from jpa query
     */
    protected String getLuceneQueryFromJPAQuery()
    {
        StringBuffer sb = new StringBuffer();

        EntityMetadata metadata = getKunderaQuery().getEntityMetadata();

        Metamodel metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        EntityType entity = metaModel.entity(metadata.getEntityClazz());
        for (Object object : kunderaQuery.getFilterClauseQueue())
        {
            if (object instanceof FilterClause)
            {
                boolean appended = false;
                FilterClause filter = (FilterClause) object;
                sb.append("+");
                // property
                sb.append(metadata.getIndexName());
                sb.append(".");
                sb.append(filter.getProperty());

                // joiner
                String appender = "";
                if (filter.getCondition().equals("="))
                {
                    sb.append(":");
                }
                else if (filter.getCondition().equalsIgnoreCase("like"))
                {
                    sb.append(":");
                    appender = "*";
                }
                else if (filter.getCondition().equalsIgnoreCase(">"))
                {
                    String fieldName = metadata.getFieldName(filter.getProperty());
                    // ;
                    sb.append(appendRange(filter.getValue().toString(), false, true, entity.getAttribute(fieldName)
                            .getJavaType()));
                    appended = true;
                }
                else if (filter.getCondition().equalsIgnoreCase(">="))
                {
                    String fieldName = metadata.getFieldName(filter.getProperty());
                    // entity.getAttribute(fieldName).getJavaType();
                    sb.append(appendRange(filter.getValue().toString(), true, true, entity.getAttribute(fieldName)
                            .getJavaType()));
                    appended = true;
                }
                else if (filter.getCondition().equalsIgnoreCase("<"))
                {
                    String fieldName = metadata.getFieldName(filter.getProperty());
                    // entity.getAttribute(fieldName).getJavaType();
                    sb.append(appendRange(filter.getValue().toString(), false, false, entity.getAttribute(fieldName)
                            .getJavaType()));
                    appended = true;
                }
                else if (filter.getCondition().equalsIgnoreCase("<="))
                {
                    String fieldName = metadata.getFieldName(filter.getProperty());
                    // entity.getAttribute(fieldName).getJavaType();
                    sb.append(appendRange(filter.getValue().toString(), true, false, entity.getAttribute(fieldName)
                            .getJavaType()));
                    appended = true;
                }

                // value. if not already appended.
                if (!appended)
                {
                    sb.append(filter.getValue());
                    sb.append(appender);
                }
            }
            else
            {
                sb.append(" " + object + " ");
            }
        }

        // add Entity_CLASS field too.
        if (sb.length() > 0)
        {
            sb.append(" AND ");
        }
        sb.append("+");
        sb.append(DocumentIndexer.ENTITY_CLASS_FIELD);
        sb.append(":");
        // sb.append(getEntityClass().getName());
        sb.append(kunderaQuery.getEntityClass().getCanonicalName().toLowerCase());

        return sb.toString();
    }

    /**
     * Returns lucene based query.
     * 
     * @param clazzFieldName
     *            lucene field name for class
     * @param clazzName
     *            class name
     * @param idFieldName
     *            lucene id field name
     * @param idFieldValue
     *            lucene id field value
     * @return query lucene query.
     */
    protected static String getQuery(String clazzFieldName, String clazzName, String idFieldName, String idFieldValue)
    {
        StringBuffer sb = new StringBuffer("+");
        sb.append(clazzFieldName);
        sb.append(":");
        sb.append(clazzName);
        sb.append(" AND ");
        sb.append("+");
        sb.append(idFieldName);
        sb.append(":");
        sb.append(idFieldValue);
        return sb.toString();
    }

    /**
     * Transform.
     * 
     * @param m
     *            the m
     * @param ls
     *            the ls
     * @param resultList
     *            the result list
     */
    protected void transform(EntityMetadata m, List<EnhanceEntity> ls, List resultList)
    {
        for (Object r : resultList)
        {
            EnhanceEntity e = new EnhanceEntity(r, PropertyAccessorHelper.getId(r, m), null);
            ls.add(e);
        }
    }

    /**
     * Fetch data from lucene.
     * 
     * @param client
     *            the client
     * @return the sets the
     */
    protected Set<String> fetchDataFromLucene(Client client)
    {
        String luceneQuery = getLuceneQueryFromJPAQuery();
        // use lucene to query and get Pk's only.
        // go to client and get relation with values.!
        // populate EnhanceEntity
        Map<String, Object> results = client.getIndexManager().search(luceneQuery);
        Set rSet = new HashSet(results.values());
        return rSet;
    }

    /**
     * On association using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param ls
     *            the ls
     */
    protected void onAssociationUsingLucene(EntityMetadata m, Client client, List<EnhanceEntity> ls)
    {
        Set<String> rSet = fetchDataFromLucene(client);

        List resultList = client.findAll(m.getEntityClazz(), rSet.toArray(new String[] {}));
        transform(m, ls, resultList);

    }

    /**
     * Append range.
     * 
     * @param value
     *            the value
     * @param inclusive
     *            the inclusive
     * @param isGreaterThan
     *            the is greater than
     * @return the string
     */
    protected String appendRange(String value, boolean inclusive, boolean isGreaterThan, Class clazz)
    {
        String appender = " ";
        StringBuilder sb = new StringBuilder();
        sb.append(":");
        sb.append(inclusive ? "[" : "{");
        sb.append(isGreaterThan ? value : "*");
        sb.append(appender);
        sb.append("TO");
        sb.append(appender);
        if (clazz.isAssignableFrom(int.class) || clazz.isAssignableFrom(Integer.class)
                || clazz.isAssignableFrom(short.class) || clazz.isAssignableFrom(long.class)
                || clazz.isAssignableFrom(Long.class) || clazz.isAssignableFrom(float.class)
                || clazz.isAssignableFrom(Float.class) || clazz.isAssignableFrom(BigDecimal.class)
                || clazz.isAssignableFrom(BigInteger.class))
        {
            sb.append(isGreaterThan ? "*" : value);

        }
        else
        {
            sb.append(isGreaterThan ? "null" : value);
        }
        sb.append(inclusive ? "]" : "}");
        return sb.toString();
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
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        EntityMetadata m = null;
        if (appMetadata.isNative(getJPAQuery()))
        {
            Class clazz = appMetadata.getMappedClass(getJPAQuery());
            m = KunderaMetadataManager.getEntityMetadata(clazz);
        }
        else
        {
            m = kunderaQuery.getEntityMetadata();
        }
        return m;
    }

    /**
     * Performs delete or update based on query.
     * 
     * @param results
     *            list of objects to be merged/deleted.
     */
    private void onDeleteOrUpdate(List results)
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

                            // Column column =
                            // entityMetadata.getColumn(columnName);
                            //
                            // if (column != null)
                            // {
                            PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), c.getValue()
                                    .toString());
                            // }
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
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
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setLockMode(javax.persistence.LockModeType)
     */
    @Override
    public Query setLockMode(LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getLockMode()
     */
    @Override
    public LockModeType getLockMode()
    {
        throw new NotImplementedException("TODO");
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
        for (Parameter p : parameters)
        {
            if (name.equals(p.getName()))
            {
                return p;
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
        if (appMetadata.isNative(query))
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
        if (parameter.getParameterType() != null && parameter.getParameterType().equals(paramClass))
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

}
