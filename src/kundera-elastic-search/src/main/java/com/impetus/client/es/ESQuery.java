/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.Query;
import javax.persistence.metamodel.EntityType;

import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.AndExpression;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.GroupByClause;
import org.eclipse.persistence.jpa.jpql.parser.HavingClause;
import org.eclipse.persistence.jpa.jpql.parser.IdentificationVariable;
import org.eclipse.persistence.jpa.jpql.parser.NullExpression;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.OrderByClause;
import org.eclipse.persistence.jpa.jpql.parser.OrderByItem;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.ListIterable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class ESQuery.
 * 
 * @author vivek.mishra Implementation of query interface {@link Query}.
 * @param <E>
 *            the element type
 */

public class ESQuery<E> extends QueryImpl
{
    /** The es filter builder. */
    private ESFilterBuilder esFilterBuilder;

    /** The aggregations key list. */
    private Set<String> aggregationsKeySet;

    /** The log. */
    private static Logger logger = LoggerFactory.getLogger(ESClient.class);

    /**
     * Instantiates a new ES query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public ESQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
        this.esFilterBuilder = new ESFilterBuilder(getKunderaQuery(), kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entity = metaModel.entity(m.getEntityClazz());
        Expression whereExpression = KunderaQueryUtils.getWhereClause(kunderaQuery.getJpqlExpression());

        QueryBuilder filter = whereExpression == null || whereExpression instanceof NullExpression ? null
                : esFilterBuilder.populateFilterBuilder(((WhereClause) whereExpression).getConditionalExpression(), m);

        return ((ESClient) client).executeQuery(filter, buildAggregation(kunderaQuery, m, filter), m,
                getKunderaQuery(),this.firstResult, this.maxResult);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List result = populateEntities(m, client);
        return setRelationEntities(result, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new ESEntityReader(kunderaQuery, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator<E> iterate()
    {
        return null;
    }

    /**
     * Gets the es filter builder.
     * 
     * @return ES Filter Builder
     */
    public ESFilterBuilder getEsFilterBuilder()
    {
        return esFilterBuilder;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#findUsingLucene(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        throw new UnsupportedOperationException("select colummn via lucene is unsupported in Elasticsearch");
    }

    /**
     * Use aggregation.
     * 
     * @param query
     *            the query
     * @param entityMetadata
     *            the entity metadata
     * @param filter
     *            the filter
     * @return the filter aggregation builder
     */
    public AggregationBuilder buildAggregation(KunderaQuery query, EntityMetadata entityMetadata, QueryBuilder filter)
    {
        SelectStatement selectStatement = query.getSelectStatement();

        // To apply filter for where clause
        AggregationBuilder aggregationBuilder = buildWhereAggregations(entityMetadata, filter);
        if (KunderaQueryUtils.hasGroupBy(query.getJpqlExpression()))
        {
            TermsBuilder termsBuilder = processGroupByClause(selectStatement.getGroupByClause(), entityMetadata, query);
            aggregationBuilder.subAggregation(termsBuilder);
        }
        else
        {
            if (KunderaQueryUtils.hasHaving(query.getJpqlExpression()))
            {
                logger.error("Identified having clause without group by, Throwing not supported operation Exception");
                throw new UnsupportedOperationException(
                        "Currently, Having clause without group by caluse is not supported.");
            }
            else
            {
                aggregationBuilder = (selectStatement != null) ? query.isAggregated() ? buildSelectAggregations(
                        aggregationBuilder, selectStatement, entityMetadata) : null : null;
            }
        }

        return aggregationBuilder;
    }

    /**
     * Adds the having clauses.
     * 
     * @param havingExpression
     *            the having expression
     * @param aggregationBuilder
     *            the aggregation builder
     * @param entityMetadata
     *            the entity metadata
     * @return the aggregation builder
     */
    private AggregationBuilder addHavingClause(Expression havingExpression, AggregationBuilder aggregationBuilder,
            EntityMetadata entityMetadata)
    {
        if (havingExpression instanceof ComparisonExpression)
        {
            Expression expression = ((ComparisonExpression) havingExpression).getLeftExpression();
            if (!isAggregationExpression(expression))
            {
                logger.error("Having clause conditions over non metric aggregated are not supported.");
                throw new UnsupportedOperationException(
                        "Currently, Having clause without Metric aggregations are not supported.");
            }

            return checkIfKeyExists(expression.toParsedText()) ? aggregationBuilder
                    .subAggregation(getMetricsAggregation(expression, entityMetadata)) : aggregationBuilder;
        }
        else if (havingExpression instanceof AndExpression)
        {
            AndExpression andExpression = (AndExpression) havingExpression;
            addHavingClause(andExpression.getLeftExpression(), aggregationBuilder, entityMetadata);
            addHavingClause(andExpression.getRightExpression(), aggregationBuilder, entityMetadata);

            return aggregationBuilder;
        }
        else if (havingExpression instanceof OrExpression)
        {
            OrExpression orExpression = (OrExpression) havingExpression;
            addHavingClause(orExpression.getLeftExpression(), aggregationBuilder, entityMetadata);
            addHavingClause(orExpression.getRightExpression(), aggregationBuilder, entityMetadata);

            return aggregationBuilder;
        }
        else
        {
            logger.error(havingExpression + "not supported in having clause.");
            throw new UnsupportedOperationException(havingExpression + "not supported in having clause.");
        }
    }

    /**
     * Process group by clause.
     * 
     * @param expression
     *            the expression
     * @param entityMetadata
     *            the entity metadata
     * @param query
     *            the query
     * @return the terms builder
     */
    private TermsBuilder processGroupByClause(Expression expression, EntityMetadata entityMetadata, KunderaQuery query)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        Expression groupByClause = ((GroupByClause) expression).getGroupByItems();

        if (groupByClause instanceof CollectionExpression)
        {
            logger.error("More than one item found in group by clause.");
            throw new UnsupportedOperationException("Currently, Group By on more than one field is not supported.");
        }

        SelectStatement selectStatement = query.getSelectStatement();

        // To apply terms and tophits aggregation to serve group by
        String jPAField = KunderaCoreUtils.getJPAColumnName(groupByClause.toParsedText(), entityMetadata, metaModel);
        TermsBuilder termsBuilder = AggregationBuilders.terms(ESConstants.GROUP_BY).field(jPAField).size(0);

        // Hard coded value for a max number of record that a group can contain.
        TopHitsBuilder topHitsBuilder = getTopHitsAggregation(selectStatement, null, entityMetadata);
        termsBuilder.subAggregation(topHitsBuilder);

        // To apply the metric aggregations (Min, max... etc) in select clause
        buildSelectAggregations(termsBuilder, query.getSelectStatement(), entityMetadata);

        if (KunderaQueryUtils.hasHaving(query.getJpqlExpression()))
        {
            addHavingClause(((HavingClause) selectStatement.getHavingClause()).getConditionalExpression(),
                    termsBuilder, entityMetadata);
        }

        if (KunderaQueryUtils.hasOrderBy(query.getJpqlExpression()))
        {
            processOrderByClause(termsBuilder, KunderaQueryUtils.getOrderByClause(query.getJpqlExpression()),
                    groupByClause, entityMetadata);
        }

        return termsBuilder;
    }

    /**
     * Process order by clause.
     * 
     * @param termsBuilder
     *            the terms builder
     * @param orderByExpression
     *            the order by expression
     * @param groupByClause
     *            the group by clause
     * @param entityMetadata
     *            the entity metadata
     */
    private void processOrderByClause(TermsBuilder termsBuilder, Expression orderByExpression,
            Expression groupByClause, EntityMetadata entityMetadata)
    {
        Expression orderByClause = (OrderByClause) orderByExpression;
        OrderByItem orderByItems = (OrderByItem) ((OrderByClause) orderByClause).getOrderByItems();

        if (orderByClause instanceof CollectionExpression
                || !(orderByItems.getExpression().toParsedText().equalsIgnoreCase(groupByClause.toParsedText())))
        {
            logger.error("Order by and group by on different field are not supported simultaneously");
            throw new UnsupportedOperationException(
                    "Order by and group by on different field are not supported simultaneously");
        }

        String ordering = orderByItems.getOrdering().toString();
        termsBuilder.order(Terms.Order.term(!ordering.equalsIgnoreCase(Expression.DESC)));
    }

    /**
     * Gets the top hits aggregation.
     * 
     * @param selectStatement
     *            the select statement
     * @param size
     *            the size
     * @param entityMetadata
     *            the entity metadata
     * @return the top hits aggregation
     */
    private TopHitsBuilder getTopHitsAggregation(SelectStatement selectStatement, Integer size,
            EntityMetadata entityMetadata)
    {
        TopHitsBuilder topHitsBuilder = AggregationBuilders.topHits(ESConstants.TOP_HITS);
        if (size != null)
        {
            topHitsBuilder.setSize(size);
        }

        return topHitsBuilder;
    }

    /**
     * Builds the where aggregations.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param filter
     *            the filter
     * @return the filter aggregation builder
     */
    private FilterAggregationBuilder buildWhereAggregations(EntityMetadata entityMetadata, QueryBuilder filter)
    {
        filter = filter != null ? filter : QueryBuilders.matchAllQuery();
        FilterAggregationBuilder filteragg = AggregationBuilders.filter(ESConstants.AGGREGATION_NAME).filter(filter);

        return filteragg;
    }

    /**
     * Builds the select aggregations.
     * 
     * @param selectStatement
     *            the select statement
     * @param entityMetadata
     *            the entity metadata
     * @param filter
     *            the filter
     * @return the filter aggregation builder
     */
    private AggregationBuilder buildSelectAggregations(AggregationBuilder aggregationBuilder,
            SelectStatement selectStatement, EntityMetadata entityMetadata)
    {
        Expression expression = ((SelectClause) selectStatement.getSelectClause()).getSelectExpression();

        if (expression instanceof CollectionExpression)
        {
            aggregationBuilder = appendAggregation((CollectionExpression) expression, entityMetadata,
                    aggregationBuilder);
        }
        else
        {
            if (isAggregationExpression(expression) && checkIfKeyExists(expression.toParsedText()))
                aggregationBuilder.subAggregation(getMetricsAggregation(expression, entityMetadata));
        }
        return aggregationBuilder;
    }

    /**
     * Append aggregation.
     * 
     * @param collectionExpression
     *            the collection expression
     * @param entityMetadata
     *            the entity metadata
     * @param nestedAggregation
     * @param aggregationBuilder
     *            the aggregation builder
     * @return the filter aggregation builder
     */
    private AggregationBuilder appendAggregation(CollectionExpression collectionExpression,
            EntityMetadata entityMetadata, AggregationBuilder aggregationBuilder)
    {
        ListIterable<Expression> functionlist = collectionExpression.children();
        for (Expression function : functionlist)
        {
            if (isAggregationExpression(function) && checkIfKeyExists(function.toParsedText()))
            {
                aggregationBuilder.subAggregation(getMetricsAggregation(function, entityMetadata));
            }
        }
        return aggregationBuilder;
    }

    /**
     * Gets the aggregation.
     * 
     * @param expression
     *            the expression
     * @param entityMetadata
     *            the entity metadata
     * @return the aggregation
     */
    private MetricsAggregationBuilder getMetricsAggregation(Expression expression, EntityMetadata entityMetadata)
    {
        AggregateFunction function = (AggregateFunction) expression;
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        String jPAColumnName = KunderaCoreUtils.getJPAColumnName(function.toParsedText(), entityMetadata, metaModel);

        MetricsAggregationBuilder aggregationBuilder = null;

        switch (function.getIdentifier())
        {
        case Expression.MIN:
            aggregationBuilder = AggregationBuilders.min(function.toParsedText()).field(jPAColumnName);
            break;
        case Expression.MAX:
            aggregationBuilder = AggregationBuilders.max(function.toParsedText()).field(jPAColumnName);
            break;
        case Expression.SUM:
            aggregationBuilder = AggregationBuilders.sum(function.toParsedText()).field(jPAColumnName);
            break;
        case Expression.AVG:
            aggregationBuilder = AggregationBuilders.avg(function.toParsedText()).field(jPAColumnName);
            break;
        case Expression.COUNT:
            aggregationBuilder = AggregationBuilders.count(function.toParsedText()).field(jPAColumnName);
            break;
        }
        return aggregationBuilder;
    }

    /**
     * Check expression.
     * 
     * @param expression
     *            the expression
     * @return true, if successful
     */
    private boolean isAggregationExpression(Expression expression)
    {
        return !(expression instanceof StateFieldPathExpression || expression instanceof IdentificationVariable);
    }

    /**
     * Checks whether key exist in ES Query.
     * 
     * @param key
     *            the key
     * @return true, if is new key
     */
    private boolean checkIfKeyExists(String key)
    {
        if (aggregationsKeySet == null)
        {
            aggregationsKeySet = new HashSet<String>();
        }
        return aggregationsKeySet.add(key);
    }
}