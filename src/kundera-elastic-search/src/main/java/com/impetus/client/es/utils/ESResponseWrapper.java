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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.SnapshotCloneListIterable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
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
    private static Logger log = LoggerFactory.getLogger(ESResponseWrapper.class);

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
        log.debug("Response of query: " + response);
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
                Object entity = null;
                for (SearchHit hit : hits.getHits())
                {
                    entity = KunderaCoreUtils.createNewInstance(clazz);
                    Map<String, Object> hitResult = hit.sourceAsMap();
                    results.add(wrap(hitResult, entityType, entity, entityMetadata, false));
                }
            }
        }
        else
        {
            results = parseAggregatedResponse(response, query, metaModel, clazz);
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
     * @return the list
     */
    private List parseAggregatedResponse(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz)
    {
        List temp = new ArrayList<>(), results = new ArrayList<>();
        InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap()
                .get(ESConstants.aggName)).getAggregations();
        Iterator<Expression> itr = getSelectExpressionOrder(query);

        while (itr.hasNext())
        {
            Expression exp = itr.next();
            String text = exp.toActualText();
            String field = KunderaQueryUtils.isAggregatedExpression(exp) ? text.substring(
                    text.indexOf(ESConstants.dot) + 1, text.indexOf(ESConstants.rightBracket)) : text.substring(
                    text.indexOf(ESConstants.dot) + 1, text.length());

            temp.add(KunderaQueryUtils.isAggregatedExpression(exp) ? getAggregatedResult(internalAggs,
                    ((AggregateFunction) exp).getIdentifier(), text, exp) : response.getHits().getAt(0).getFields()
                    .get(((AbstractAttribute) metaModel.entity(clazz).getAttribute(field)).getJPAColumnName())
                    .getValue());

        }

        for (Object value : temp)
        {
            if (!value.toString().equalsIgnoreCase(ESConstants.infinity))
            {
                results.add(value);
            }
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
     * @return the map
     */
    public Map<String, Object> parseAggregations(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz)
    {
        Map<String, Object> aggMap = new HashMap<String, Object>();
        if (query.isAggregated() == true && response.getAggregations() != null)
        {
            InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap()
                    .get(ESConstants.aggName)).getAggregations();
            Iterator<Expression> itr = getSelectExpressionOrder(query);

            while (itr.hasNext())
            {
                Expression exp = itr.next();
                if (AggregateFunction.class.isAssignableFrom(exp.getClass()))
                {
                    Object value = getAggregatedResult(internalAggs, ((AggregateFunction) exp).getIdentifier(),
                            exp.toParsedText(), exp);
                    if (!value.toString().equalsIgnoreCase(ESConstants.infinity))
                    {
                        aggMap.put(exp.toParsedText(), Double.valueOf(value.toString()));
                    }
                }
            }
        }
        return aggMap;
    }

    /**
     * Gets the aggregated result.
     * 
     * @param internalAggs
     *            the internal aggs
     * @param identifier
     *            the identifier
     * @param field
     *            the field
     * @param exp
     *            the exp
     * @return the aggregated result
     */
    private Object getAggregatedResult(InternalAggregations internalAggs, String identifier, String field,
            Expression exp)
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
    public Iterator<Expression> getSelectExpressionOrder(KunderaQuery query)
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
            return new SnapshotCloneListIterable<Expression>(list).iterator();
        }
        else
        {
            return selectExpression.children().iterator();
        }
    }
}