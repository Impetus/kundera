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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.SnapshotCloneListIterable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;

import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.KunderaQuery;

/**
 * @author Amit Kumar
 * 
 */
public class ESResponseReader
{

    /**
     * @param response
     * @param query
     * @param metaModel
     * @param clazz
     * @return
     */
    public List parseAggregatedResponse(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz)
    {
        List temp = new ArrayList<>(), results = new ArrayList<>();
        InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap().get("whereClause"))
                .getAggregations();
        Iterator<Expression> itr = getSelectExpressionOrder(query);

        while (itr.hasNext())
        {

            Expression exp = itr.next();
            String text = exp.toActualText();
            String field = isAggregated(exp) ? text.substring(text.indexOf('.') + 1, text.indexOf(')')) : text
                    .substring(text.indexOf('.') + 1, text.length());

            temp.add(isAggregated(exp) ? getAggregatedResult(internalAggs, ((AggregateFunction) exp).getIdentifier(),
                    text, exp) : response.getHits().getAt(0).getFields()
                    .get(((AbstractAttribute) metaModel.entity(clazz).getAttribute(field)).getJPAColumnName())
                    .getValue());

        }
        for (Object value : temp)
        {
            if (!value.toString().equalsIgnoreCase("INFINITY"))
            {
                results.add(value);
            }
        }
        return results;
    }

    /**
     * @param query
     * @return
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

    /**
     * @param expression
     * @return
     */
    private boolean isAggregated(Expression expression)
    {
        return AggregateFunction.class.isAssignableFrom(expression.getClass()) ? true : false;
    }

    /**
     * @param internalAggs
     * @param identifier
     * @param field
     * @param exp
     * @return result value of aggregation
     */
    private Double getAggregatedResult(InternalAggregations internalAggs, String identifier, String field,
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
        }
        return null;
    }

    /**
     * @param response
     * @param query
     * @param metaModel
     * @param clazz
     * @return
     */
    public Map<String, Object> parseAggregations(SearchResponse response, KunderaQuery query, MetamodelImpl metaModel,
            Class clazz)
    {
        Map<String, Object> aggMap = new HashMap<String, Object>();
        if (query.isAggregated() == true && response.getAggregations() != null)
        {
            InternalAggregations internalAggs = ((InternalFilter) response.getAggregations().getAsMap()
                    .get("whereClause")).getAggregations();
            Iterator<Expression> itr = getSelectExpressionOrder(query);

            while (itr.hasNext())
            {
                Expression exp = itr.next();
                if (AggregateFunction.class.isAssignableFrom(exp.getClass()))
                {
                    Object value = getAggregatedResult(internalAggs, ((AggregateFunction) exp).getIdentifier(),
                            exp.toParsedText(), exp);
                    if (!value.toString().equalsIgnoreCase("INFINITY"))
                    {
                        aggMap.put(exp.toParsedText(), Double.valueOf(value.toString()));
                    }
                }
            }
        }
        return aggMap;
    }
}