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
import java.util.Queue;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.Parameter;
import javax.persistence.PersistenceException;
import javax.persistence.PostLoad;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.index.IndexingConstants;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.DefaultEntityType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class QueryImpl.
 * 
 * @author vivek.mishra
 * @param <E>
 *            the element type
 */
public abstract class QueryImpl<E> implements Query, com.impetus.kundera.query.Query
{
    /** The kundera query. */
    protected KunderaQuery kunderaQuery;

    /** The persistence delegeator. */
    protected PersistenceDelegator persistenceDelegeator;

    /** The kundera metadata. */
    protected KunderaMetadata kunderaMetadata;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(QueryImpl.class);

    /** The parameters. */
    private Set<Parameter<?>> parameters;

    /** The hints. */
    private Map<String, Object> hints = new HashMap<String, Object>();

    /**
     * Default maximum result to fetch.
     */
    protected int maxResult = 100;

    /** The first result. */
    protected int firstResult = 0;

    /** The fetch size. */
    private Integer fetchSize;

    /** The is single result. */
    protected boolean isSingleResult = false;

    /** The ttl. */
    protected Integer ttl;

    /**
     * Instantiates a new query impl.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public QueryImpl(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        this.kunderaQuery = kunderaQuery;
        this.persistenceDelegeator = persistenceDelegator;
        this.kunderaMetadata = kunderaMetadata;
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

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#executeUpdate()
     */
    @Override
    public int executeUpdate()
    {
        return onExecuteUpdate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getResultList()
     */
    @Override
    public List<?> getResultList()
    {
        if (log.isDebugEnabled())
            log.info("On getResultList() executing query: " + getJPAQuery());

        // as per JPA post event should happen before fetching data from
        // database.
        List results = null;

        if (getEntityMetadata() == null)
        {
            // Scalar Query
            if (kunderaQuery.isDeleteUpdate())
            {
                executeUpdate();
            }
            else
            {
                Client client = persistenceDelegeator.getClient(kunderaQuery.getPersistenceUnit());
                results = populateEntities(null, client);
            }
        }
        else
        {
            handlePostEvent();

            if (kunderaQuery.isDeleteUpdate())
            {
                executeUpdate();
            }
            else
            {
                results = fetch();
                assignReferenceToProxy(results);
            }
        }
        return results != null ? results : new ArrayList();
    }

    /**
     * Sets the relation entities.
     * 
     * @param enhanceEntities
     *            the enhance entities
     * @param client
     *            the client
     * @param m
     *            the m
     * @return the list
     */
    protected List<Object> setRelationEntities(List enhanceEntities, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        List<Object> result = new ArrayList<Object>();
        // Stack of objects. To be used for referring any similar object found
        // later.
        // This prevents infinite recursive loop and hence prevents stack
        // overflow.
        Map<Object, Object> relationStack = new HashMap<Object, Object>();

        if (enhanceEntities != null)
        {
            for (Object e : enhanceEntities)
            {
                addToRelationStack(relationStack, e, m);
            }
        }

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
                        persistenceDelegeator, false, relationStack));

            }
        }

        return result;
    }

    // Adds an object to the stack for referring
    /**
     * Adds the to relation stack.
     * 
     * @param relationStack
     *            the relation stack
     * @param entity
     *            the entity
     * @param m
     *            the m
     */
    protected void addToRelationStack(Map<Object, Object> relationStack, Object entity, EntityMetadata m)
    {
        Object obj = entity;
        if (entity instanceof EnhanceEntity)
        {
            obj = ((EnhanceEntity) entity).getEntity();
        }
        relationStack.put(obj.getClass().getCanonicalName() + "#" + PropertyAccessorHelper.getId(obj, m), obj);

    }

    /**
     * Populate using lucene for embeddeId.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param result
     *            the result
     * @param searchFilter
     *            the search filter
     * @param metaModel
     *            the meta model
     * @return the list
     */
    private List<Object> populateEmbeddedIdUsingLucene(EntityMetadata m, Client client, List<Object> result,
            Map<String, Object> searchFilter, MetamodelImpl metaModel)
    {
        List<Object> compositeIds = new ArrayList<Object>();

        for (String compositeIdName : searchFilter.keySet())
        {
            Object compositeId = null;
            Map<String, String> uniquePKs = (Map<String, String>) searchFilter.get(compositeIdName);
            compositeId = KunderaCoreUtils.initialize(m.getIdAttribute().getBindableJavaType(), compositeId);

            prepareCompositeIdObject(m.getIdAttribute(), compositeId, uniquePKs, metaModel);
            compositeIds.add(compositeId);
        }
        return findUsingLucene(m, client, compositeIds.toArray());
    }

    /**
     * Prepare composite id object.
     * 
     * @param attribute
     *            the attribute
     * @param compositeId
     *            the composite id
     * @param uniquePKs
     *            the unique p ks
     * @param metaModel
     *            the meta model
     * @return the object
     */
    private Object prepareCompositeIdObject(final SingularAttribute attribute, Object compositeId,
            Map<String, String> uniquePKs, MetamodelImpl metaModel)
    {
        Field[] fields = attribute.getBindableJavaType().getDeclaredFields();
        EmbeddableType embeddable = metaModel.embeddable(attribute.getBindableJavaType());

        for (Field field : attribute.getBindableJavaType().getDeclaredFields())
        {
            if (!ReflectUtils.isTransientOrStatic(field))
            {
                if (metaModel.isEmbeddable(((AbstractAttribute) embeddable.getAttribute(field.getName()))
                        .getBindableJavaType()))
                {
                    try
                    {
                        field.setAccessible(true);
                        Object embeddedObject = prepareCompositeIdObject(
                                (SingularAttribute) embeddable.getAttribute(field.getName()),
                                KunderaCoreUtils.initialize(((AbstractAttribute) embeddable.getAttribute(field
                                        .getName())).getBindableJavaType(), field.get(compositeId)), uniquePKs,
                                metaModel);
                        PropertyAccessorHelper.set(compositeId, field, embeddedObject);
                    }
                    catch (IllegalAccessException e)
                    {
                        log.error(e.getMessage());
                    }
                }
                else
                {
                    PropertyAccessorHelper.set(
                            compositeId,
                            field,
                            PropertyAccessorHelper.fromSourceToTargetClass(field.getType(), String.class,
                                    uniquePKs.get(field.getName())));
                }
            }
        }
        return compositeId;
    }

    /**
     * find data using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param primaryKeys
     *            the primary keys
     * @return the list
     */
    private List<Object> findUsingLucene(EntityMetadata m, Client client, Object[] primaryKeys)
    {

        String idField = m.getIdAttribute().getName();
        String equals = "=";
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        String columnName = ((AbstractAttribute) entityType.getAttribute(idField)).getJPAColumnName();
        List<Object> result = new ArrayList<Object>();
        Queue queue = getKunderaQuery().getFilterClauseQueue();
        KunderaQuery kunderaQuery = getKunderaQuery();

        for (Object primaryKey : primaryKeys)
        {
            FilterClause filterClause = kunderaQuery.new FilterClause(columnName, equals, primaryKey, idField);
            kunderaQuery.setFilter(kunderaQuery.getEntityAlias() + "." + columnName + " = " + primaryKey);
            queue.clear();
            queue.add(filterClause);
            List<Object> object = findUsingLucene(m, client);
            if (object != null && !object.isEmpty())
                result.add(object.get(0));
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
        Set<Object> uniquePKs = null;

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        if (client.getIndexManager().getIndexer().getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
        {
            String luceneQ = KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery, kunderaMetadata);

            Map<String, Object> searchFilter = client.getIndexManager().search(m.getEntityClazz(), luceneQ,
                    Constants.INVALID, Constants.INVALID);
            // Map<String, Object> searchFilter =
            // client.getIndexManager().search(kunderaMetadata, kunderaQuery,
            // persistenceDelegeator, m);
            boolean isEmbeddedId = metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType());

            if (isEmbeddedId)
            {
                return populateEmbeddedIdUsingLucene(m, client, result, searchFilter, metaModel);
            }

            Object[] primaryKeys = searchFilter.values().toArray(new Object[] {});
            // Object[] primaryKeys =
            // ((List)searchFilter.get("primaryKeys")).toArray(new Object[] {});

            uniquePKs = new HashSet<Object>(Arrays.asList(primaryKeys));
            return findUsingLucene(m, client, uniquePKs.toArray());
        }
        else
        {
            return populateUsingElasticSearch(client, m);

        }

    }

    /**
     * Populate using elastic search.
     * 
     * @param client
     *            the client
     * @param EntityMetadata
     *            the m
     * @return Result list by fetching from ES
     */
    private List populateUsingElasticSearch(Client client, EntityMetadata m)
    {
        Map<String, Object> searchFilter = client.getIndexManager().search(kunderaMetadata, kunderaQuery,
                persistenceDelegeator, m, this.firstResult, this.maxResult);
        Object[] primaryKeys = ((Map<String, Object>) searchFilter.get(Constants.PRIMARY_KEYS)).values().toArray(
                new Object[] {});
        Map<String, Object> aggregations = (Map<String, Object>) searchFilter.get(Constants.AGGREGATIONS);

        Iterable<Expression> resultOrderIterable = (Iterable<Expression>) searchFilter
                .get(Constants.SELECT_EXPRESSION_ORDER);
        List<Object> results = new ArrayList<Object>();

        if (!kunderaQuery.isAggregated())
        {
            results.addAll(findUsingLucene(m, client, primaryKeys));
        }
        else
        {
            if (KunderaQueryUtils.hasGroupBy(kunderaQuery.getJpqlExpression()))
            {
                populateGroupByResponse(aggregations, resultOrderIterable, results, client, m);
            }
            else
            {
                Iterator<Expression> resultOrder = resultOrderIterable.iterator();
                while (resultOrder.hasNext())
                {
                    Expression expression = (Expression) resultOrder.next();

                    if (AggregateFunction.class.isAssignableFrom(expression.getClass()))
                    {
                        if (aggregations.get(expression.toParsedText()) != null)
                        {
                            results.add(aggregations.get(expression.toParsedText()));
                        }
                    }
                    else
                    {
                        results.addAll(findUsingLucene(m, client, new Object[] { primaryKeys[0] }));
                    }
                }
            }
        }

        return results;
    }

    /**
     * Populate group by response.
     * 
     * @param aggregations
     *            the aggregations
     * @param resultOrderIterable
     *            the result order iterable
     * @param results
     *            the results
     * @param client
     *            the client
     * @param m
     *            the m
     */
    private void populateGroupByResponse(Map<String, Object> aggregations, Iterable<Expression> resultOrderIterable,
            List<Object> results, Client client, EntityMetadata m)
    {
        List temp;
        Object entity = null;
        for (String entry : aggregations.keySet())
        {
            entity = null;
            Object obj = aggregations.get(entry);
            temp = new ArrayList<>();
            Iterator<Expression> resultOrder = resultOrderIterable.iterator();
            while (resultOrder.hasNext())
            {
                Expression expression = (Expression) resultOrder.next();

                if (AggregateFunction.class.isAssignableFrom(expression.getClass()))
                {
                    if (((Map) obj).get(expression.toParsedText()) != null)
                    {
                        temp.add(((Map) obj).get(expression.toParsedText()));
                    }
                }
                else
                {
                    if (entity == null)
                    {
                        entity = findUsingLucene(m, client, new Object[] { entry }).get(0);
                    }
                    temp.add(getEntityFieldValue(m, entity, expression.toParsedText()));
                }
            }
            results.add(temp.size() == 1 ? temp.get(0) : temp);
        }
    }

    /**
     * Gets the entity field value.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param field
     *            the field
     * @return the entity field value
     */
    private Object getEntityFieldValue(EntityMetadata entityMetadata, Object entity, String field)
    {
        Class clazz = entityMetadata.getEntityClazz();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(clazz);

        if (field.indexOf(".") > 0 && entityMetadata.getEntityClazz().equals(entity.getClass()))
        {
            String fieldName = field.substring(field.indexOf(".") + 1, field.length());
            Attribute attribute = entityType.getAttribute(fieldName);
            return PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
        }
        else
        {
            // for hbase v2 client (sends arraylist for specific fields)
            if (entity instanceof ArrayList)
            {
                Object element = ((ArrayList) entity).get(0);
                ((ArrayList) entity).remove(0);
                return element;
            }
            else
            {
                return entity;
            }
        }
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

    /**
     * Find using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
    protected abstract List findUsingLucene(EntityMetadata m, Client client);

    /**
     * Recursively populate entities.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
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
     * @return the int
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

    /**
     * On update delete event.
     * 
     * @return the int
     */
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
                    PersistenceCacheManager.addEntityToPersistenceCache(result, persistenceDelegeator,
                            PropertyAccessorHelper.getId(result, this.getEntityMetadata()));

                    persistenceDelegeator.remove(result);
                }
            }
            else
            {
                EntityMetadata entityMetadata = getEntityMetadata();
                for (Object result : results)
                {
                    PersistenceCacheManager.addEntityToPersistenceCache(result, persistenceDelegeator,
                            PropertyAccessorHelper.getId(result, this.getEntityMetadata()));

                    for (UpdateClause c : kunderaQuery.getUpdateClauseQueue())
                    {
                        String columnName = c.getProperty();
                        try
                        {

                            DefaultEntityType entityType = (DefaultEntityType) kunderaMetadata.getApplicationMetadata()
                                    .getMetamodel(entityMetadata.getPersistenceUnit())
                                    .entity(entityMetadata.getEntityClazz());

                            // That will always be attribute name.

                            Attribute attribute = entityType.getAttribute(columnName);

                            // TODO : catch column name.

                            if (c.getValue() instanceof String)
                            {
                                PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), c.getValue()
                                        .toString());
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

    /**
     * *********************** Methods from {@link Query} interface
     * ******************************.
     * 
     * @return the single result
     */

    /* @see javax.persistence.Query#getSingleResult() */
    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getSingleResult()
     */
    @Override
    public Object getSingleResult()
    {
        // to fetch a single result form database.
        isSingleResult = true;
        List results = getResultList();
        isSingleResult = false;
        return onReturnResults(results);
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
        this.firstResult = startPosition;
        return this;
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
     * {@link Query}.
     * 
     * @param hintName
     *            the hint name
     * @param value
     *            the value
     * @return the query
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
     * Returns a {@link Map} containing query hints set by user.
     * 
     * @return the hints
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
            if (parameters == null)
            {
                parameters = new HashSet<Parameter<?>>();
            }
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
     * Returns specific parameter instance for given name.
     * 
     * @param name
     *            parameter name.
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
     * Returns parameter by ordinal.
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
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
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
        if (parameter != null && parameter.getParameterType() != null
                && parameter.getParameterType().equals(paramClass))
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
    private List<Object> onParameterValue(String paramString)
    {
        List<Object> value = kunderaQuery.getClauseValue(paramString);
        if (value == null)
        {
            throw new IllegalStateException("parameter has not been bound" + paramString);
        }
        return value;
    }

    /**
     * Gets the columns.
     * 
     * @param columns
     *            the columns
     * @param m
     *            the m
     * @return the columns
     */
    protected String[] getColumns(final String[] columns, final EntityMetadata m)
    {
        List<String> columnAsList = new ArrayList<String>();
        if (columns != null && columns.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.Query#setFetchSize(java.lang.Integer)
     */
    public void setFetchSize(Integer fetchsize)
    {
        this.fetchSize = fetchsize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.Query#getFetchSize()
     */
    public Integer getFetchSize()
    {
        return this.fetchSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.Query#applyTTL(int)
     */
    public void applyTTL(int ttlInSeconds)
    {
        this.ttl = ttlInSeconds;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.Query#close()
     */
    public abstract void close();

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.Query#iterate()
     */
    public abstract <E> Iterator<E> iterate();

    /**
     * Handle post event callbacks.
     * 
     */
    protected void handlePostEvent()
    {
        EntityMetadata metadata = getEntityMetadata();
        if (!kunderaQuery.isDeleteUpdate())
        {
            persistenceDelegeator.getEventDispatcher().fireEventListeners(metadata, null, PostLoad.class);
        }
    }

    /**
     * Returns collection of fetched entities.
     * 
     * @return the list
     */
    protected List fetch()
    {
        EntityMetadata metadata = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(metadata);
        List results = isRelational(metadata) ? recursivelyPopulateEntities(metadata, client) : populateEntities(
                metadata, client);
        return results;
    }

    /**
     * On validate single result.
     * 
     * @param results
     *            the results
     */
    protected void onValidateSingleResult(List results)
    {
        if (results == null || results.isEmpty())
        {
            log.error("No result found for {} ", kunderaQuery.getJPAQuery());
            throw new NoResultException("No result found!");
        }

        if (results.size() > 1)
        {
            log.error("Non unique results found for query {} ", kunderaQuery.getJPAQuery());
            throw new NonUniqueResultException("Containing more than one result!");
        }

    }

    /**
     * On return results.
     * 
     * @param results
     *            the results
     * @return the object
     */
    protected Object onReturnResults(List results)
    {
        onValidateSingleResult(results);
        return results.get(0);
    }

    /**
     * Returns true, if associated entity holds relational references(e.g. @OneToMany
     * etc.) else false.
     * 
     * @param m
     *            entity metadata
     * 
     * @return true, if holds relation else false
     */
    private boolean isRelational(EntityMetadata m)
    {
        // if related via join table OR contains relations.
        return m.isRelationViaJoinTable() || (m.getRelationNames() != null && (!m.getRelationNames().isEmpty()));
    }

    /**
     * If returned collection of object holds a reference to.
     * 
     * @param results
     *            the results
     */
    private void assignReferenceToProxy(List results)
    {
        if (results != null)
        {
            for (Object obj : results)
            {
                kunderaMetadata.getCoreMetadata().getLazyInitializerFactory().setProxyOwners(getEntityMetadata(), obj);
            }
        }
    }
}