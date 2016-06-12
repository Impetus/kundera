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
package com.impetus.client.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.eclipse.persistence.jpa.jpql.parser.AdditionExpression;
import org.eclipse.persistence.jpa.jpql.parser.AndExpression;
import org.eclipse.persistence.jpa.jpql.parser.BetweenExpression;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.DivisionExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.IdentificationVariable;
import org.eclipse.persistence.jpa.jpql.parser.InExpression;
import org.eclipse.persistence.jpa.jpql.parser.InputParameter;
import org.eclipse.persistence.jpa.jpql.parser.LikeExpression;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.MultiplicationExpression;
import org.eclipse.persistence.jpa.jpql.parser.NullExpression;
import org.eclipse.persistence.jpa.jpql.parser.NumericLiteral;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.SubExpression;
import org.eclipse.persistence.jpa.jpql.parser.SubtractionExpression;
import org.elasticsearch.index.query.AndQueryBuilder;
import org.elasticsearch.index.query.NotQueryBuilder;
import org.elasticsearch.index.query.OrQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * The Class ESFilterBuilder.
 * 
 * @author Amit Kumar
 */
public class ESFilterBuilder
{
    /** The Kundera query. */
    private KunderaQuery kunderaQuery;

    /** The Kundera metadata. */
    private KunderaMetadata kunderaMetadata;

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(ESFilterBuilder.class);

    /**
     * Instantiates a new ES filter builder.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public ESFilterBuilder(KunderaQuery kunderaQuery, KunderaMetadata kunderaMetadata)
    {
        this.kunderaQuery = kunderaQuery;
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * Populate filter builder.
     * 
     * @param condtionalExp
     *            the condtional exp
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @return the filter builder
     */
    public QueryBuilder populateFilterBuilder(Expression condtionalExp, EntityMetadata m)
    {
        log.info("Populating filter for expression: " + condtionalExp);
        QueryBuilder filter = null;

        if (condtionalExp instanceof SubExpression)
        {
            filter = populateFilterBuilder(((SubExpression) condtionalExp).getExpression(), m);
        }
        else if (condtionalExp instanceof ComparisonExpression)
        {
            filter = getFilter(populateFilterClause((ComparisonExpression) condtionalExp), m);
        }
        else if (condtionalExp instanceof BetweenExpression)
        {
            filter = populateBetweenFilter((BetweenExpression) condtionalExp, m);
        }
        else if (condtionalExp instanceof LogicalExpression)
        {
            filter = populateLogicalFilterBuilder(condtionalExp, m);
        }
        else if (condtionalExp instanceof LikeExpression)
        {
            filter = populateLikeQuery((LikeExpression) condtionalExp, m);
        }
        else if (condtionalExp instanceof InExpression)
        {
            filter = populateInQuery((InExpression) condtionalExp, m);
        }
        else
        {
            log.error(condtionalExp.toParsedText() + "found in where clause. Not supported in elasticsearch.");
            throw new KunderaException(condtionalExp.toParsedText() + " not supported in ElasticSearch");
        }

        log.debug("Following is the populated filter for required query: " + filter);
        return filter;
    }

    /**
     * Populate like query.
     * 
     * @param likeExpression
     *            the like expression
     * @param metadata
     *            the metadata
     * @return the filter builder
     */
    private QueryBuilder populateLikeQuery(LikeExpression likeExpression, EntityMetadata metadata)
    {
        Expression patternValue = likeExpression.getPatternValue();
        String field = likeExpression.getStringExpression().toString();

        String likePattern = (patternValue instanceof InputParameter) ? kunderaQuery.getParametersMap()
                .get((patternValue).toParsedText()).toString() : patternValue.toParsedText().toString();
        String jpaField = getField(field);

        log.debug("Pattern value for field " + field + " is: " + patternValue);
        QueryBuilder filterBuilder = getQueryBuilder(kunderaQuery.new FilterClause(jpaField, Expression.LIKE,
                likePattern, field), metadata);

        return filterBuilder;
    }

    /**
     * Populate IN query filter.
     * 
     * @param inExpression
     *            the in expression
     * @param metadata
     *            the metadata
     * @return the filter builder
     */
    private QueryBuilder populateInQuery(InExpression inExpression, EntityMetadata metadata)
    {
        String property = getField(inExpression.getExpression().toParsedText());
        Expression inItemsParameter = inExpression.getInItems();

        log.debug("IN query parameters for field " + property + " is: " + inItemsParameter);
        Iterable inItemsIterable = getInValuesCollection(inItemsParameter);

        return getFilter(kunderaQuery.new FilterClause(property, Expression.IN, inItemsIterable, property), metadata);
    }

    /**
     * Returns the collection of IN clause values.
     * 
     * @param inClauseValues
     *            the in clause values
     * @return the in values collection
     */
    private Collection getInValuesCollection(Expression inClauseValues)
    {
        Collection inParameterCollection = new ArrayList<>();
        if (inClauseValues instanceof NullExpression)
        {
            log.debug("No items passed in IN clause values, returning blank IN values list");
            return inParameterCollection;
        }

        if (inClauseValues instanceof InputParameter)
        {
            Object inValues = kunderaQuery.getParametersMap().get(inClauseValues.toParsedText());
            log.debug(inClauseValues.toParsedText() + "named parameter found in query, Replacing parameter with "
                    + inValues);

            inParameterCollection = inValues.getClass().isArray() ? Arrays.asList((Object[]) inValues)
                    : (Collection) kunderaQuery.getParametersMap().get(inClauseValues.toParsedText());

            return inParameterCollection;
        }

        if (inClauseValues instanceof CollectionExpression)
        {
            Iterator inValueIterator = ((CollectionExpression) inClauseValues).children().iterator();

            log.debug("Collection object found for IN clause values");
            while (inValueIterator.hasNext())
            {
                Expression value = (Expression) inValueIterator.next();
                inParameterCollection.add(value.toParsedText());
            }
            return inParameterCollection;
        }

        throw new KunderaException(inClauseValues.toParsedText() + " not supported for IN clause");
    }

    /**
     * Gets the field.
     * 
     * @param fieldValue
     *            the field value
     * @return the field
     */
    private String getField(String fieldValue)
    {
        return fieldValue.substring(fieldValue.indexOf(ESConstants.DOT) + 1, fieldValue.length());
    }

    /**
     * Populate between filter.
     * 
     * @param betweenExpression
     *            the between expression
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @return the filter builder
     */
    private QueryBuilder populateBetweenFilter(BetweenExpression betweenExpression, EntityMetadata m)
    {
        String lowerBoundExpression = getBetweenBoundaryValues(betweenExpression.getLowerBoundExpression());
        String upperBoundExpression = getBetweenBoundaryValues(betweenExpression.getUpperBoundExpression());
        String field = getField(betweenExpression.getExpression().toParsedText());

        log.debug("Between clause for field " + field + "with lower bound " + lowerBoundExpression + "and upper bound "
                + upperBoundExpression);

        return new AndQueryBuilder(getFilter(kunderaQuery.new FilterClause(field, Expression.GREATER_THAN_OR_EQUAL,
                lowerBoundExpression, field), m), getFilter(kunderaQuery.new FilterClause(field,
                Expression.LOWER_THAN_OR_EQUAL, upperBoundExpression,field), m));
    }

    /**
     * Gets the between boundary values.
     * 
     * @param boundExpression
     *            the bound expression
     * @return the between boundry values
     */
    private String getBetweenBoundaryValues(Expression boundExpression)
    {
        if (boundExpression instanceof IdentificationVariable || boundExpression instanceof NumericLiteral
                || boundExpression instanceof InputParameter)
        {
            Object value = (boundExpression instanceof InputParameter) ? kunderaQuery.getParametersMap().get(
                    (boundExpression).toParsedText()) : boundExpression.toParsedText();
            return value.toString();
        }
        else if (boundExpression instanceof AdditionExpression)
        {
            String leftValue = checkInputParameter(((AdditionExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((AdditionExpression) boundExpression).getRightExpression());

            return new Integer(Integer.parseInt(leftValue) + Integer.parseInt(rightValue)).toString();
        }
        else if (boundExpression instanceof SubtractionExpression)
        {
            String leftValue = checkInputParameter(((SubtractionExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((SubtractionExpression) boundExpression).getRightExpression());

            return new Integer(Integer.parseInt(leftValue) - Integer.parseInt(rightValue)).toString();
        }
        else if (boundExpression instanceof MultiplicationExpression)
        {
            String leftValue = checkInputParameter(((MultiplicationExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((MultiplicationExpression) boundExpression).getRightExpression());

            return new Integer(Integer.parseInt(leftValue) * Integer.parseInt(rightValue)).toString();
        }

        else if (boundExpression instanceof DivisionExpression)
        {
            String leftValue = checkInputParameter(((DivisionExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((DivisionExpression) boundExpression).getRightExpression());
            return new Integer(Integer.parseInt(leftValue) / Integer.parseInt(rightValue)).toString();
        }

        return null;
    }

    /**
     * Check input parameter.
     * 
     * @param expression
     *            the expression
     * @return the string
     */
    private String checkInputParameter(Expression expression)
    {
        return (expression instanceof InputParameter) ? kunderaQuery.getParametersMap()
                .get((expression).toParsedText()).toString() : expression.toParsedText().toString();
    }

    /**
     * Populate logical filter builder.
     * 
     * @param logicalExp
     *            the logical exp
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @return the filter builder
     */
    private QueryBuilder populateLogicalFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        String identifier = ((LogicalExpression) logicalExp).getIdentifier();

        return (identifier.equalsIgnoreCase(LogicalExpression.AND)) ? getAndFilterBuilder(logicalExp, m) : (identifier
                .equalsIgnoreCase(LogicalExpression.OR)) ? getOrFilterBuilder(logicalExp, m) : null;
    }

    /**
     * Gets the and filter builder.
     * 
     * @param logicalExp
     *            the logical exp
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @return the and filter builder
     */
    private AndQueryBuilder getAndFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        AndExpression andExp = (AndExpression) logicalExp;
        Expression leftExpression = andExp.getLeftExpression();
        Expression rightExpression = andExp.getRightExpression();

        return new AndQueryBuilder(populateFilterBuilder(leftExpression, m), populateFilterBuilder(rightExpression, m));
    }

    /**
     * Gets the or filter builder.
     * 
     * @param logicalExp
     *            the logical exp
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @return the or filter builder
     */
    private OrQueryBuilder getOrFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        OrExpression orExp = (OrExpression) logicalExp;
        Expression leftExpression = orExp.getLeftExpression();
        Expression rightExpression = orExp.getRightExpression();

        return new OrQueryBuilder(populateFilterBuilder(leftExpression, m), populateFilterBuilder(rightExpression, m));
    }

    /**
     * Populate filter clause.
     * 
     * @param conditionalExpression
     *            the conditional expression
     * @return the filter clause
     */
    private FilterClause populateFilterClause(ComparisonExpression conditionalExpression)
    {
        String property = ((StateFieldPathExpression) conditionalExpression.getLeftExpression()).getPath(1);
        String condition = conditionalExpression.getComparisonOperator();
        Expression rightExpression = conditionalExpression.getRightExpression();
        Object value = (rightExpression instanceof InputParameter) ? kunderaQuery.getParametersMap().get(
                (rightExpression).toParsedText()) : rightExpression.toParsedText();

        return (condition != null && property != null) ? kunderaQuery.new FilterClause(property, condition, value, property)
                : null;
    }

    /**
     * Gets the filter.
     * 
     * @param clause
     *            the clause
     * @param metadata
     *            the metadata
     * @param entityType
     *            the entity type
     * @return the filter
     */
    private QueryBuilder getFilter(FilterClause clause, final EntityMetadata metadata)
    {
        String condition = clause.getCondition();
        Object value = condition.equals(Expression.IN) ? clause.getValue() : clause.getValue().get(0);
        String name = ((AbstractAttribute) ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit())).entity(metadata.getEntityClazz()).getAttribute(clause.getProperty()))
                .getJPAColumnName();

        QueryBuilder filterBuilder = null;

        if (condition.equals(Expression.EQUAL))
        {
            filterBuilder = new TermQueryBuilder(name, value);
        }
        else if (condition.equals(Expression.GREATER_THAN))
        {
            filterBuilder = new RangeQueryBuilder(name).gt(value);
        }
        else if (condition.equals(Expression.LOWER_THAN))
        {
            filterBuilder = new RangeQueryBuilder(name).lt(value);
        }
        else if (condition.equals(Expression.GREATER_THAN_OR_EQUAL))
        {
            filterBuilder = new RangeQueryBuilder(name).gte(value);
        }
        else if (condition.equals(Expression.LOWER_THAN_OR_EQUAL))
        {
            filterBuilder = new RangeQueryBuilder(name).lte(value);
        }
        else if (condition.equals(Expression.DIFFERENT))
        {
            filterBuilder = new NotQueryBuilder(new TermQueryBuilder(name, value));
        }
        else if (condition.equals(Expression.IN))
        {
            filterBuilder = new TermsQueryBuilder(name, clause.getValue());
        }

        return filterBuilder;
    }

    /**
     * Gets the query.
     * 
     * @param clause
     *            the clause
     * @param metadata
     *            the metadata
     * @return the query
     */
    private QueryBuilder getQueryBuilder(FilterClause clause, final EntityMetadata metadata)
    {
        String condition = clause.getCondition();
        String value = clause.getValue().get(0).toString();
        String name = ((AbstractAttribute) ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit())).entity(metadata.getEntityClazz()).getAttribute(clause.getProperty()))
                .getJPAColumnName();

        String likePattern = value.contains(ESConstants.PERCENTAGE) ? value.replaceAll(ESConstants.PERCENTAGE,
                ESConstants.ASTERISK) : ESConstants.ASTERISK + value + ESConstants.ASTERISK;

        QueryBuilder queryBuilder = null;
        if (condition.equals(Expression.LIKE))
        {
            queryBuilder = QueryBuilders.wildcardQuery(name, likePattern);
        }

        return QueryBuilders.queryFilter(queryBuilder);
    }
}
