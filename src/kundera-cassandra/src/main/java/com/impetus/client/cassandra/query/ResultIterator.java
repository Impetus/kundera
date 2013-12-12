/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.cassandra.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.metamodel.EmbeddableType;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author vivek.mishra .
 * 
 *         Implementation of Cassandra result iteration.
 * 
 *         TODO::: Need to add support for relational entities and a junit for
 *         Composite key test
 */
class ResultIterator<E> implements IResultIterator<E>
{
    private static Logger log = LoggerFactory.getLogger(ResultIterator.class);

    private CassQuery query;

    private EntityMetadata entityMetadata;

    private Client client;

    private EntityReader reader;

    private int maxResult = 1;

    private List<E> results;

    private byte[] start;

    private static final String MIN_ = "min";

    private static final String MAX_ = "max";

    // private boolean isValidated;

    private int fetchSize;

    private int count;

    private boolean scrollComplete;

    private Map<String, Object> externalProperties;

    private E current;

    /**
     * Constructor with parameters
     * 
     * @param query
     * @param m
     * @param client
     * @param reader
     * @param fetchSize
     */
    ResultIterator(final Query query, final EntityMetadata m, final Client client, final EntityReader reader,
            final int fetchSize)
    {
        this.client = client;
        this.query = (CassQuery) query;
        this.entityMetadata = m;
        this.reader = reader;
        scrollComplete = false;
        this.fetchSize = fetchSize;

    }

    @Override
    public boolean hasNext()
    {
        if (checkOnFetchSize())
        {
            onCheckRelation();
            if (!checkOnEmptyResult())
            {
                scrollComplete = true;
                return false;
            }

            return true;
        }

        return false;
    }

    @Override
    public E next()
    {
        if (current != null && checkOnEmptyResult() && current.equals(results.get(results.size() - 1)))
        {
            hasNext();
        }

        if (scrollComplete)
        {
            throw new NoSuchElementException("Nothing to scroll further for:" + entityMetadata.getEntityClazz());
        }

        E lastFetchedEntity = getEntity(results.get(results.size() - 1));
        start = lastFetchedEntity != null ? idValueInByteArr() : null;
        current = getEntity(results.get(results.size() - 1));

        return current;

    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove method is not supported over pagination");
    }

    @Override
    public List<E> next(int chunkSize)
    {
        throw new UnsupportedOperationException("fetch in chunks is not yet supported");
    }

    /**
     * Check on fetch size. returns true, if count on fetched rows is less than
     * fetch size.
     * 
     * @return
     */
    private boolean checkOnFetchSize()
    {
        if (count++ < fetchSize)
        {
            return true;
        }
        count = 0;
        scrollComplete = true;
        return false;
    }

    /**
     * on check relation event, invokes populate entities and set relational
     * entities, in case relations are present.
     */
    private void onCheckRelation()
    {
        try
        {
            results = populateEntities(entityMetadata, client);

            if (entityMetadata.isRelationViaJoinTable()
                    || (entityMetadata.getRelationNames() != null && !(entityMetadata.getRelationNames().isEmpty())))
            {
                query.setRelationalEntities(results, client, entityMetadata);
            }
        }
        catch (Exception e)
        {
            throw new PersistenceException("Error while scrolling over results, Caused by :.", e);
        }

    }

    /**
     * Method parse provided JPQL query into: 1. CQL3 query, in case cql3 is
     * enabled or is a native query. 2. list of index clause, if cql2 is
     * enabled. Then executes query for given min & max values for scrolling
     * over results.
     * 
     * @param m
     *            entity metadata
     * @param client
     *            client
     * @return list of database values wrapped into entities.
     * @throws Exception
     *             throws exception, in case of run time error.
     */
    private List<E> populateEntities(EntityMetadata m, Client client) throws Exception
    {
        if (log.isDebugEnabled())
        {
            log.debug("Populating entities for Cassandra query {}.", ((QueryImpl) query).getJPAQuery());
        }
        List<E> result = new ArrayList<E>();
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        // if id attribute is embeddable, it is meant for CQL translation.
        // make it independent of embedded stuff and allow even to add non
        // composite into where clause and let cassandra complain for it.

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        String queryString = appMetadata.getQuery(((QueryImpl) query).getJPAQuery());

        boolean isNative = ((CassQuery) query).isNative();

        if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m))
        {
            String parsedQuery = query.onQueryOverCQL3(m, client, metaModel, null);

            parsedQuery = appendWhereClauseWithScroll(parsedQuery);
            results = parsedQuery != null ? ((CassandraClientBase) client).executeQuery(m.getEntityClazz(),
                    m.getRelationNames(), parsedQuery) : null;
        }
        else
        {
            if (isNative)
            {
                final String nativeQuery = appendWhereClauseWithScroll(queryString != null ? queryString
                        : ((QueryImpl) query).getJPAQuery());
                results = nativeQuery != null ? ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), null,
                        nativeQuery) : null;
            }
            else
            {
                // Index in Inverted Index table if applicable
                boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
                        MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
                Map<Boolean, List<IndexClause>> ixClause = query.prepareIndexClause(m, useInvertedIndex);
                if (useInvertedIndex && !((QueryImpl) query).getKunderaQuery().getFilterClauseQueue().isEmpty())
                {
                    result = (List) ((CassandraEntityReader) this.reader).readFromIndexTable(m, client, ixClause);
                }
                else
                {
                    boolean isRowKeyQuery = ixClause.keySet().iterator().next();

                    List<IndexExpression> expressions = !ixClause.get(isRowKeyQuery).isEmpty() ? ixClause
                            .get(isRowKeyQuery).get(0).getExpressions() : null;

                    Map<String, byte[]> rowKeys = ((CassandraEntityReader) this.reader).getRowKeyValue(expressions,
                            ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName());

                    byte[] minValue = start == null ? rowKeys.get(MIN_) : start;
                    byte[] maxVal = rowKeys.get(MAX_);

                    results = ((CassandraClientBase) client).findByRange(minValue, maxVal, m,
                            m.getRelationNames() != null && !m.getRelationNames().isEmpty(), m.getRelationNames(),
                            query.getColumnList(m, ((QueryImpl) query).getKunderaQuery().getResult(), null),
                            expressions, maxResult);

                    if (maxResult == 1)
                    {
                        maxResult++;
                    }
                    else if (maxResult > 1 && checkOnEmptyResult() && maxResult != results.size())
                    {
                        // means iterating over last record only, so need for
                        // database trip anymore!.
                        results = null;
                    }
                }
            }
        }
        return results;
    }

    /**
     * Appends where claues and prepare for next fetch. Method to be called in
     * case cql3 enabled.
     * 
     * @param parsedQuery
     *            parsed query.
     * 
     * @return cql3 query to be executed.
     */
    private String appendWhereClauseWithScroll(String parsedQuery)
    {
        String queryWithoutLimit = parsedQuery.replaceAll(
                parsedQuery.substring(parsedQuery.lastIndexOf(CQLTranslator.LIMIT), parsedQuery.length()), "");

        CQLTranslator translator = new CQLTranslator();

        final String tokenCondition = prepareNext(translator, queryWithoutLimit);

        StringBuilder builder = new StringBuilder(queryWithoutLimit);

        if (tokenCondition != null)
        {
            if (query.getKunderaQuery().getFilterClauseQueue().isEmpty())
            {
                builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
            }
            else
            {
                builder.append(CQLTranslator.AND_CLAUSE);
            }
            builder.append(tokenCondition);
        }

        String replaceQuery = replaceAndAppendLimit(builder.toString());
        builder.replace(0, builder.toString().length(), replaceQuery);
        translator.buildFilteringClause(builder);

        // in case of fetch by ID, token condition will be null and results will
        // not be empty.
        return checkOnEmptyResult() && tokenCondition == null ? null : builder.toString();
    }

    /**
     * Replace and append limit.
     * 
     * @param parsedQuery
     *            parsed cql3 query.
     * 
     * @return cql3 query appended with limit clause.
     */
    private String replaceAndAppendLimit(String parsedQuery)
    {
        // String queryWithoutLimit = parsedQuery.replaceAll(
        // parsedQuery.substring(parsedQuery.lastIndexOf(CQLTranslator.LIMIT),
        // parsedQuery.length()), "");
        StringBuilder builder = new StringBuilder(parsedQuery);
        onLimit(builder);
        parsedQuery = builder.toString();
        return parsedQuery;
    }

    /**
     * Append limit to sql3 query.
     * 
     * @param builder
     *            builder instance.
     */
    private void onLimit(StringBuilder builder)
    {
        builder.append(CQLTranslator.LIMIT);
        builder.append(this.maxResult);
    }

    /**
     * Parse and append cql3 token function for iter.next() call.
     * 
     * @param translator
     *            cql translator.
     * 
     * @return parsed/append cql3 query.
     */
    private String prepareNext(CQLTranslator translator, String query)
    {
        if (checkOnEmptyResult())
        {
            String idName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
            Map<Boolean, String> filterOnId = getConditionOnIdColumn(idName);

            if (filterOnId.get(true) != null)
            {
                String condition = filterOnId.get(true);
                // means id clause present in query.
                // now if id attribute is embeddable then fetch partition key
                // for token
                // if condition is with equals then no need to go for another
                // fetch.

                if (condition.equals("="))
                {
                    // no need to fetch another record, as there will be only
                    // one
                    return null;
                }
                else if (condition.endsWith(">") || condition.equals(">="))
                {
                    query = replaceAppliedToken(query);
                    return query;
                }
            }

            // Means there is an previous entity.
            Object entity = results.get(results.size() - 1);
            Class idClazz = ((AbstractAttribute) entityMetadata.getIdAttribute()).getBindableJavaType();
            Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
            StringBuilder builder = new StringBuilder(CQLTranslator.TOKEN);
            translator
                    .appendColumnName(builder, CassandraUtilities.getIdColumnName(entityMetadata, externalProperties)/* idName */);
            builder.append(CQLTranslator.CLOSE_BRACKET);
            builder.append(" > ");
            builder.append(CQLTranslator.TOKEN);
            translator.appendValue(builder, idClazz, id, false, false);
            builder.append(CQLTranslator.CLOSE_BRACKET);
            return builder.toString();
        }
        return null;
    }

    private Map<Boolean, String> getConditionOnIdColumn(String idColumn)
    {

        Map<Boolean, String> filterIdResult = new HashMap<Boolean, String>();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EmbeddableType keyObj = null;
        if (metaModel.isEmbeddable(entityMetadata.getIdAttribute().getBindableJavaType()))
        {
            keyObj = metaModel.embeddable(entityMetadata.getIdAttribute().getBindableJavaType());
        }

        boolean isPresent = false;
        for (Object o : query.getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                String fieldName = clause.getProperty();
                String condition = clause.getCondition();
                Object value = clause.getValue();

                if (keyObj != null && fieldName.equals(idColumn)
                        || (keyObj != null && StringUtils.contains(fieldName, '.')) || (idColumn.equals(fieldName)))
                {
                    filterIdResult.put(true, condition);
                    break;
                }
            }
        }

        return filterIdResult;
    }

    /**
     * id attribute's value in byte[]
     * 
     * @return id attribute's value in byte[]
     */
    private byte[] idValueInByteArr()
    {
        Object entity = results.get(results.size() - 1);
        Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
        String idName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        Class idClazz = ((AbstractAttribute) entityMetadata.getIdAttribute()).getBindableJavaType();
        Bytes bytes = query.getBytesValue(idName, entityMetadata, id);

        return bytes.toByteArray();

    }

    /**
     * check if result list is null or empty. Returns true, if it is not empty
     * or null.
     * 
     * @return boolean value (true/false).
     * 
     */
    private boolean checkOnEmptyResult()
    {
        return results != null && !results.isEmpty();
    }

    /**
     * Extract wrapped entity object from enhanced entity.
     * 
     * @param entity
     *            enhanced entity.
     * 
     * @return returns extracted instance of E.
     */
    private E getEntity(Object entity)
    {
        return (E) (entity.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) entity).getEntity()
                : entity);
    }

    private String replaceAppliedToken(String query)
    {
        final String tokenRegex = "\\btoken\\(";
        final String pattern = "#TOKENKUNDERA#"; // need to replace with this as
                                                 // pattern matcher was
                                                 // returning false.
        query = query.replaceAll(tokenRegex, pattern);

        if (query.indexOf(pattern) > -1) // Means token( has been present and
                                         // replaced.
        {
            CQLTranslator translator = new CQLTranslator();

            int closingIndex = query.indexOf(CQLTranslator.CLOSE_BRACKET, query.lastIndexOf(pattern));

            String object = query.substring(query.lastIndexOf(pattern) + pattern.length(), closingIndex);

            Object entity = results.get(results.size() - 1);
            Class idClazz = ((AbstractAttribute) entityMetadata.getIdAttribute()).getBindableJavaType();
            Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
            StringBuilder builder = new StringBuilder();

            translator.appendValue(builder, idClazz, id, false, false);

            query = query.replaceAll(pattern + object, pattern + builder.toString());
            query = query.replaceAll(pattern, CQLTranslator.TOKEN);
        }

        return query;
    }
}
