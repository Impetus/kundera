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
package com.impetus.client.es.utils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;

import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.AndExpression;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.HavingClause;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.NullExpression;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.ListIterable;
import org.eclipse.persistence.jpa.jpql.utility.iterable.SnapshotCloneListIterable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.es.ESConstants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class ESResponseWrapper.
 * 
 * @author Amit Kumar
 * 
 */
public final class ESResponseWrapper
{
    /** log for this class. */
    private static Logger logger = LoggerFactory.getLogger(ESResponseWrapper.class);

    /**
     * Parses the response.
     * 
     * @param response
     *            the response
     * @param aggregation
     *            the aggregation
     * @param fieldsToSelect
     *            the fields to select
     * @param metaModel
     *            the meta model
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     * @param query
     *            the query
     * @return the list
     */
    public List parseResponse(SearchResponse response, AbstractAggregationBuilder aggregation, String[] fieldsToSelect,
            MetamodelImpl metaModel, Class clazz, final EntityMetadata entityMetadata, KunderaQuery query)
    {
        logger.debug("Response of query: " + response);

        List results = new ArrayList();
        EntityType entityType = metaModel.entity(clazz);

        if (aggregation == null)
        {
            SearchHits hits = response.getHits();
            if (fieldsToSelect != null && fieldsToSelect.length > 1 && !(fieldsToSelect[1] == null))
            {
                for (SearchHit hit : hits.getHits())
                {
                    if (fieldsToSelect.length == 2)
                    {
                        results.add(hit
                                .getFields()
                                .get(((AbstractAttribute) metaModel.entity(clazz).getAttribute(fieldsToSelect[1]))
                                        .getJPAColumnName()).getValue());
                    }
                    else
                    {
                        List temp = new ArrayList();

                        for (int i = 1; i < fieldsToSelect.length; i++)
                        {
                            temp.add(hit
                                    .getFields()
                                    .get(((AbstractAttribute) metaModel.entity(clazz).getAttribute(fieldsToSelect[i]))
                                            .getJPAColumnName()).getValue());
                        }
                        results.add(temp);
                    }
                }
            }
            else
            {
                results = getEntityObjects(clazz, entityMetadata, entityType, hits);
            }
        }
        else
        {
            results = parseAggregatedResponse(response, query, metaModel, clazz, entityMetadata);
        }
        return results;
    }

    /**
     * Parses the aggregated response.
     * 
     * @param response
     *            the response
     * @param query
     *            the query
     * @param metaModel
     *            the meta model
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     * @return the list
     */
    private List parseAggregatedResponse(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz, EntityMetadata entityMetadata)
    {
        List results, temp = new ArrayList<>();
        InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap()
                .get(ESConstants.AGGREGATION_NAME)).getAggregations();

        if (query.isSelectStatement() && KunderaQueryUtils.hasGroupBy(query.getJpqlExpression()))
        {
            Terms buckets = (Terms) (internalAggs).getAsMap().get(ESConstants.GROUP_BY);

            filterBuckets(buckets, query);

            results = onIterateBuckets(buckets, query, metaModel, clazz, entityMetadata);
        }
        else
        {
            results = new ArrayList<>();
            temp = buildRecords(internalAggs, response.getHits(), query, metaModel, clazz, entityMetadata);

            for (Object value : temp)
            {
                if (!value.toString().equalsIgnoreCase(ESConstants.INFINITY))
                {
                    results.add(value);
                }
            }
        }
        return results;
    }

    /**
     * On iterate buckets.
     * 
     * @param buckets
     *            the buckets
     * @param query
     *            the query
     * @param metaModel
     *            the meta model
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     * @return the list
     */
    private List onIterateBuckets(Terms buckets, KunderaQuery query, MetamodelImpl metaModel, Class clazz,
            EntityMetadata entityMetadata)
    {
        List temp, results = new ArrayList<>();

        for (Terms.Bucket entry : buckets.getBuckets())
        {
            logger.debug("key [{}], doc_count [{}]", entry.getKey(), entry.getDocCount());
            Aggregations aggregations = entry.getAggregations();
            TopHits topHits = aggregations.get(ESConstants.TOP_HITS);

            temp = buildRecords((InternalAggregations) aggregations, topHits.getHits(), query, metaModel, clazz,
                    entityMetadata);
            results.add(temp.size() == 1 ? temp.get(0) : temp);
        }

        return results;
    }

    /**
     * Parses the aggregations.
     * 
     * @param response
     *            the response
     * @param query
     *            the query
     * @param metaModel
     *            the meta model
     * @param clazz
     *            the clazz
     * @param EntityMetadata
     *            the m
     * @return the map
     */
    public Map<String, Object> parseAggregations(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz, EntityMetadata m)
    {
        Map<String, Object> aggregationsMap = new LinkedHashMap<>();
        if (query.isAggregated() == true && response.getAggregations() != null)
        {
            InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap()
                    .get(ESConstants.AGGREGATION_NAME)).getAggregations();

            ListIterable<Expression> iterable = getSelectExpressionOrder(query);

            if (query.isSelectStatement() && KunderaQueryUtils.hasGroupBy(query.getJpqlExpression()))
            {
                Terms buckets = (Terms) (internalAggs).getAsMap().get(ESConstants.GROUP_BY);

                filterBuckets(buckets, query);

                for (Terms.Bucket bucket : buckets.getBuckets())
                {
                    logger.debug("key [{}], doc_count [{}]", bucket.getKey(), bucket.getDocCount());

                    TopHits topHits = bucket.getAggregations().get(ESConstants.TOP_HITS);
                    aggregationsMap.put(topHits.getHits().getAt(0).getId(),
                            buildRecords(iterable, (InternalAggregations) bucket.getAggregations()));
                }
            }
            else
            {
                aggregationsMap = buildRecords(iterable, internalAggs);
            }
        }
        return aggregationsMap;
    }

    /**
     * Builds the records.
     * 
     * @param iterable
     *            the iterable
     * @param internalAgg
     *            the internal agg
     * @return the map
     */
    private Map<String, Object> buildRecords(ListIterable<Expression> iterable, InternalAggregations internalAgg)
    {
        Map<String, Object> temp = new HashMap<>();
        Iterator<Expression> itr = iterable.iterator();

        while (itr.hasNext())
        {
            Expression exp = itr.next();
            if (AggregateFunction.class.isAssignableFrom(exp.getClass()))
            {
                Object value = getAggregatedResult(internalAgg, ((AggregateFunction) exp).getIdentifier(), exp);
                if (!value.toString().equalsIgnoreCase(ESConstants.INFINITY))
                {
                    temp.put(exp.toParsedText(), Double.valueOf(value.toString()));
                }
            }
        }

        return temp;
    }

    /**
     * Parses the records.
     * 
     * @param internalAgg
     *            the internal agg
     * @param topHits
     *            the top hits
     * @param query
     *            the query
     * @param metaModel
     *            the meta model
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     * @return the list
     */
    private List buildRecords(InternalAggregations internalAgg, SearchHits topHits, KunderaQuery query,
            MetamodelImpl metaModel, Class clazz, EntityMetadata entityMetadata)
    {
        List temp = new ArrayList<>();

        Iterator<Expression> orderIterator = getSelectExpressionOrder(query).iterator();
        while (orderIterator.hasNext())
        {
            Expression exp = orderIterator.next();
            String text = exp.toActualText();
            String field = KunderaQueryUtils.isAggregatedExpression(exp) ? text.substring(
                    text.indexOf(ESConstants.DOT) + 1, text.indexOf(ESConstants.RIGHT_BRACKET)) : text.substring(
                    text.indexOf(ESConstants.DOT) + 1, text.length());

            temp.add(KunderaQueryUtils.isAggregatedExpression(exp) ? getAggregatedResult(internalAgg,
                    ((AggregateFunction) exp).getIdentifier(), exp) : getFirstResult(query, field, topHits, clazz,
                    metaModel, entityMetadata));
        }
        return temp;
    }

    /**
     * Filter buckets.
     * 
     * @param buckets
     *            the buckets
     * @param query
     *            the query
     * @return the terms
     */
    private Terms filterBuckets(Terms buckets, KunderaQuery query)
    {
        Expression havingClause = query.getSelectStatement().getHavingClause();

        if (!(havingClause instanceof NullExpression) && havingClause != null)
        {
            Expression conditionalExpression = ((HavingClause) havingClause).getConditionalExpression();

            for (Iterator<Bucket> bucketIterator = buckets.getBuckets().iterator(); bucketIterator.hasNext();)
            {
                InternalAggregations internalAgg = (InternalAggregations) bucketIterator.next().getAggregations();
                if (!isValidBucket(internalAgg, query, conditionalExpression))
                {
                    bucketIterator.remove();
                }
            }
        }
        return buckets;
    }

    /**
     * Checks if is valid bucket.
     * 
     * @param internalAgg
     *            the internal agg
     * @param query
     *            the query
     * @param conditionalExpression
     *            the conditional expression
     * @return true, if is valid bucket
     */
    private boolean isValidBucket(InternalAggregations internalAgg, KunderaQuery query, Expression conditionalExpression)
    {
        if (conditionalExpression instanceof ComparisonExpression)
        {
            Expression expression = ((ComparisonExpression) conditionalExpression).getLeftExpression();
            Object leftValue = getAggregatedResult(internalAgg, ((AggregateFunction) expression).getIdentifier(),
                    expression);

            String rightValue = ((ComparisonExpression) conditionalExpression).getRightExpression().toParsedText();
            return validateBucket(leftValue.toString(), rightValue,
                    ((ComparisonExpression) conditionalExpression).getIdentifier());
        }
        else if (LogicalExpression.class.isAssignableFrom(conditionalExpression.getClass()))
        {
            Expression leftExpression = null, rightExpression = null;
            if (conditionalExpression instanceof AndExpression)
            {
                AndExpression andExpression = (AndExpression) conditionalExpression;
                leftExpression = andExpression.getLeftExpression();
                rightExpression = andExpression.getRightExpression();
            }
            else
            {
                OrExpression orExpression = (OrExpression) conditionalExpression;
                leftExpression = orExpression.getLeftExpression();
                rightExpression = orExpression.getRightExpression();
            }

            return validateBucket(isValidBucket(internalAgg, query, leftExpression),
                    isValidBucket(internalAgg, query, rightExpression),
                    ((LogicalExpression) conditionalExpression).getIdentifier());
        }
        else
        {
            logger.error("Expression " + conditionalExpression + " in having clause is not supported in Kundera");
            throw new UnsupportedOperationException(conditionalExpression
                    + " in having clause is not supported in Kundera");
        }
    }

    /**
     * Validate bucket.
     * 
     * @param left
     *            the left
     * @param right
     *            the right
     * @param logicalOperation
     *            the logical operation
     * @return true, if successful
     */
    private boolean validateBucket(boolean left, boolean right, String logicalOperation)
    {
        logger.debug("Logical opertation " + logicalOperation + " found in having clause");
        if (Expression.AND.equalsIgnoreCase(logicalOperation))
        {
            return left && right;
        }
        else if (Expression.OR.equalsIgnoreCase(logicalOperation))
        {
            return left || right;
        }
        else
        {
            logger.error(logicalOperation + " in having clause is not supported in Kundera");
            throw new UnsupportedOperationException(logicalOperation + " in having clause is not supported in Kundera");
        }
    }

    /**
     * Validate bucket.
     * 
     * @param left
     *            the left
     * @param right
     *            the right
     * @param operator
     *            the operator
     * @return true, if successful
     */
    private boolean validateBucket(String left, String right, String operator)
    {
        Double leftValue = Double.valueOf(left);
        Double rightValue = Double.valueOf(right);

        logger.debug("Comparison expression " + operator + "found with left value: " + left + " right value: " + right);

        if (Expression.GREATER_THAN.equals(operator))
        {
            return leftValue > rightValue;
        }
        else if (Expression.GREATER_THAN_OR_EQUAL.equals(operator))
        {
            return leftValue >= rightValue;
        }
        else if (Expression.LOWER_THAN.equals(operator))
        {
            return leftValue < rightValue;
        }
        else if (Expression.LOWER_THAN_OR_EQUAL.equals(operator))
        {
            return leftValue <= rightValue;
        }
        else if (Expression.EQUAL.equals(operator))
        {
            return leftValue == rightValue;
        }
        else
        {
            logger.error(operator + " in having clause is not supported in Kundera");
            throw new UnsupportedOperationException(operator + " in having clause is not supported in Kundera");
        }
    }

    /**
     * Gets the first result.
     * 
     * @param query
     *            the query
     * @param field
     *            the field
     * @param hits
     *            the hits
     * @param clazz
     *            the clazz
     * @param metaModel
     *            the meta model
     * @param entityMetadata
     *            the entity metadata
     * @return the first
     */
    private Object getFirstResult(KunderaQuery query, String field, SearchHits hits, Class clazz, Metamodel metaModel,
            EntityMetadata entityMetadata)
    {
        Object entity;

        if (query.getEntityAlias().equals(field))
        {
            entity = getEntityObjects(clazz, entityMetadata, metaModel.entity(clazz), hits).get(0);
        }
        else
        {
            String jpaField = ((AbstractAttribute) metaModel.entity(clazz).getAttribute(field)).getJPAColumnName();
            entity = query.getSelectStatement().hasGroupByClause() ? hits.getAt(0).sourceAsMap().get(jpaField) : hits
                    .getAt(0).getFields().get(jpaField).getValue();
        }
        return entity;
    }

    /**
     * Gets the aggregated result.
     * 
     * @param internalAggs
     *            the internal aggs
     * @param identifier
     *            the identifier
     * @param exp
     *            the exp
     * @return the aggregated result
     */
    private Object getAggregatedResult(InternalAggregations internalAggs, String identifier, Expression exp)
    {
        switch (identifier)
        {
        case Expression.MIN:
            return (((InternalMin) internalAggs.get(exp.toParsedText())).getValue());

        case Expression.MAX:
            return (((InternalMax) internalAggs.get(exp.toParsedText())).getValue());

        case Expression.AVG:
            return (((InternalAvg) internalAggs.get(exp.toParsedText())).getValue());

        case Expression.SUM:
            return (((InternalSum) internalAggs.get(exp.toParsedText())).getValue());

        case Expression.COUNT:
            return (((InternalValueCount) internalAggs.get(exp.toParsedText())).getValue());
        }

        throw new KunderaException("No support for " + identifier + " aggregation.");
    }

    /**
     * Wrap find result.
     * 
     * @param searchResults
     *            the search results
     * @param entityType
     *            the entity type
     * @param result
     *            the result
     * @param metadata
     *            the metadata
     * @param b
     *            the b
     * @return the object
     */
    public Object wrapFindResult(Map<String, Object> searchResults, EntityType entityType, Object result,
            EntityMetadata metadata, boolean b)
    {
        return wrap(searchResults, entityType, result, metadata, b);
    }

    /**
     * Wrap.
     * 
     * @param results
     *            the results
     * @param entityType
     *            the entity type
     * @param result
     *            the result
     * @param metadata
     *            the metadata
     * @param isIdSet
     *            the is id set
     * @return the object
     */
    private Object wrap(Map<String, Object> results, EntityType entityType, Object result, EntityMetadata metadata,
            boolean isIdSet)
    {

        Map<String, Object> relations = new HashMap<String, Object>();
        Object key = null;
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes)
        {
            String fieldName = ((AbstractAttribute) attribute).getJPAColumnName();

            if (!attribute.isAssociation())
            {
                Object fieldValue = results.get(fieldName);

                key = onId(key, attribute, fieldValue);

                if (!isIdSet && key != null)
                {
                    PropertyAccessorHelper.setId(result, metadata, key);
                }

                fieldValue = onEnum(attribute, fieldValue);

                // TODOO:This has to be corrected. Reason is, in case of execute
                // query over composite key. It will not work

                setField(result, key, attribute, fieldValue);
            }

            if (attribute.isAssociation())
            {
                Object fieldValue = results.get(fieldName);
                relations.put(fieldName, fieldValue);
            }
        }
        return relations.isEmpty() ? result : new EnhanceEntity(result, key, relations);
    }

    /**
     * On enum.
     * 
     * @param attribute
     *            the attribute
     * @param fieldValue
     *            the field value
     * @return the object
     */
    private Object onEnum(Attribute attribute, Object fieldValue)
    {
        if (((Field) attribute.getJavaMember()).getType().isEnum())
        {
            EnumAccessor accessor = new EnumAccessor();
            fieldValue = accessor.fromString(((AbstractAttribute) attribute).getBindableJavaType(),
                    fieldValue.toString());
        }
        return fieldValue;
    }

    /**
     * Sets the field.
     * 
     * @param result
     *            the result
     * @param key
     *            the key
     * @param attribute
     *            the attribute
     * @param fieldValue
     *            the field value
     */
    private void setField(Object result, Object key, Attribute attribute, Object fieldValue)
    {
        if (fieldValue != null)
        {
            if (((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(Date.class)
                    || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(java.sql.Date.class)
                    || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(Timestamp.class)
                    || ((AbstractAttribute) attribute).getBindableJavaType().isAssignableFrom(Calendar.class))
            {
                PropertyAccessorFactory.STRING.fromString(((AbstractAttribute) attribute).getBindableJavaType(),
                        fieldValue.toString());
            }
            else if (key == null || !key.equals(fieldValue))
            {
                PropertyAccessorHelper.set(result, (Field) attribute.getJavaMember(), fieldValue);
            }
        }
    }

    /**
     * On id.
     * 
     * @param key
     *            the key
     * @param attribute
     *            the attribute
     * @param fieldValue
     *            the field value
     * @return the object
     */
    private Object onId(Object key, Attribute attribute, Object fieldValue)
    {
        if (SingularAttribute.class.isAssignableFrom(attribute.getClass()) && ((SingularAttribute) attribute).isId())
        {
            key = fieldValue;
        }
        return key;
    }

    /**
     * Gets the select expression order.
     * 
     * @param query
     *            the query
     * @return the select expression order
     */
    public ListIterable<Expression> getSelectExpressionOrder(KunderaQuery query)
    {
        if (!KunderaQueryUtils.isSelectStatement(query.getJpqlExpression()))
        {
            return null;
        }
        Expression selectExpression = ((SelectClause) (query.getSelectStatement()).getSelectClause())
                .getSelectExpression();

        List<Expression> list;

        if (!(selectExpression instanceof CollectionExpression))
        {
            list = new LinkedList<Expression>();
            list.add(selectExpression);
            return new SnapshotCloneListIterable<Expression>(list);
        }
        else
        {
            return selectExpression.children();
        }
    }

    /**
     * Gets the entity objects.
     * 
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
     * @param entityType
     *            the entity type
     * @param hits
     *            the hits
     * @return the entity objects
     */
    private List getEntityObjects(Class clazz, final EntityMetadata entityMetadata, EntityType entityType,
            SearchHits hits)
    {
        List results = new ArrayList();

        Object entity = null;
        for (SearchHit hit : hits.getHits())
        {
            entity = KunderaCoreUtils.createNewInstance(clazz);
            Map<String, Object> hitResult = hit.sourceAsMap();
            results.add(wrap(hitResult, entityType, entity, entityMetadata, false));
        }

        return results;
    }
}