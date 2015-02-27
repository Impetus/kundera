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

import org.eclipse.persistence.jpa.jpql.parser.AdditionExpression;
import org.eclipse.persistence.jpa.jpql.parser.AndExpression;
import org.eclipse.persistence.jpa.jpql.parser.BetweenExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.DivisionExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.IdentificationVariable;
import org.eclipse.persistence.jpa.jpql.parser.InputParameter;
import org.eclipse.persistence.jpa.jpql.parser.LikeExpression;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.MultiplicationExpression;
import org.eclipse.persistence.jpa.jpql.parser.NumericLiteral;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.SubExpression;
import org.eclipse.persistence.jpa.jpql.parser.SubtractionExpression;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * @author Amit Kumar
 *
 */
public class ESFilterBuilder
{
    private KunderaQuery kunderaQuery ;
    
    private KunderaMetadata kunderaMetadata;
    
    public ESFilterBuilder(KunderaQuery kunderaQuery, KunderaMetadata kunderaMetadata)
    {
        this.kunderaQuery = kunderaQuery;
        this.kunderaMetadata = kunderaMetadata;
    }
    
    /**
     * Populate filter builder.
     *
     * @param condtionalExp the condtional exp
     * @param m the m
     * @param entity the entity
     * @return the filter builder
     */
    public FilterBuilder populateFilterBuilder(Expression condtionalExp, EntityMetadata m)
    {
        FilterBuilder filter = null;

        if (condtionalExp instanceof SubExpression)
        {
            condtionalExp = ((SubExpression) condtionalExp).getExpression();
        }

        if (condtionalExp instanceof ComparisonExpression)
        {
            filter = getFilter(populateFilterClause((ComparisonExpression) condtionalExp), m);
        }

        if (condtionalExp instanceof BetweenExpression)
        {
            filter = populateBetweenFilter((BetweenExpression) condtionalExp, m);
        }

        if (condtionalExp instanceof LogicalExpression)
        {
            filter = populateLogicalFilterBuilder(condtionalExp, m);
        }

        if (condtionalExp instanceof LikeExpression)
        {
            filter = populateLikeQuery((LikeExpression) condtionalExp, m);
        }
        
        if (filter == null)
        {
            throw new KunderaException(condtionalExp.getClass() + " not supported in ElasticSearch");
        }
        return filter;
    }

    /**
     * Populate like query.
     *
     * @param likeExpression the like expression
     * @param metadata the metadata
     * @return the filter builder
     */
    private FilterBuilder populateLikeQuery(LikeExpression likeExpression, EntityMetadata metadata)
    {
        Expression patternValue = likeExpression.getPatternValue();
        String fieldExpression = likeExpression.getStringExpression().toString();

        String likePattern = (patternValue instanceof InputParameter) ? kunderaQuery.getParametersMap()
                .get((patternValue).toParsedText()).toString() : patternValue.toParsedText().toString();
        String jpaField = fieldExpression.substring(fieldExpression.indexOf('.') + 1, fieldExpression.length());

        FilterBuilder filterBuilder = getQueryBuilder(kunderaQuery.new FilterClause(jpaField, Expression.LIKE, likePattern),
                metadata);

        return filterBuilder;
    }


    /**
     * Populate between filter.
     *
     * @param betweenExpression the between expression
     * @param m the m
     * @param entity the entity
     * @return the filter builder
     */
    private FilterBuilder populateBetweenFilter(BetweenExpression betweenExpression, EntityMetadata m)
    {

        String parsedField = betweenExpression.getExpression().toParsedText();
        String lowerBoundExpression = getBetweenBoundaryValues(betweenExpression.getLowerBoundExpression());
        String upperBoundExpression = getBetweenBoundaryValues(betweenExpression.getUpperBoundExpression());

        return new AndFilterBuilder(getFilter(
                kunderaQuery.new FilterClause(parsedField.substring(parsedField.indexOf('.') + 1,
                        parsedField.length()), ">=", lowerBoundExpression), m), getFilter(
                                kunderaQuery.new FilterClause(parsedField.substring(parsedField.indexOf('.') + 1,
                        parsedField.length()), "<=", upperBoundExpression), m));
    }

    /**
     * Gets the between boundary values.
     *
     * @param boundExpression the bound expression
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

        if (boundExpression instanceof AdditionExpression)
        {
            String leftValue = checkInputParameter(((AdditionExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((AdditionExpression) boundExpression).getRightExpression());
            return new Integer(Integer.parseInt(leftValue) + Integer.parseInt(rightValue)).toString();
        }

        if (boundExpression instanceof SubtractionExpression)
        {
            String leftValue = checkInputParameter(((SubtractionExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((SubtractionExpression) boundExpression).getRightExpression());

            return new Integer(Integer.parseInt(leftValue) - Integer.parseInt(rightValue)).toString();
        }

        if (boundExpression instanceof MultiplicationExpression)
        {
            String leftValue = checkInputParameter(((MultiplicationExpression) boundExpression).getLeftExpression());
            String rightValue = checkInputParameter(((MultiplicationExpression) boundExpression).getRightExpression());

            return new Integer(Integer.parseInt(leftValue) * Integer.parseInt(rightValue)).toString();
        }

        if (boundExpression instanceof DivisionExpression)
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
     * @param expression the expression
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
     * @param logicalExp the logical exp
     * @param m the m
     * @param entity the entity
     * @return the filter builder
     */
    private FilterBuilder populateLogicalFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        String identifier = ((LogicalExpression) logicalExp).getIdentifier();

        return (identifier.equalsIgnoreCase(LogicalExpression.AND)) ? getAndFilterBuilder(logicalExp, m)
                : (identifier.equalsIgnoreCase(LogicalExpression.OR)) ? getOrFilterBuilder(logicalExp, m)
                        : null;
    }

    /**
     * Gets the and filter builder.
     *
     * @param logicalExp the logical exp
     * @param m the m
     * @param entity the entity
     * @return the and filter builder
     */
    private AndFilterBuilder getAndFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        AndExpression andExp = (AndExpression) logicalExp;
        Expression leftExpression = andExp.getLeftExpression();
        Expression rightExpression = andExp.getRightExpression();

        return new AndFilterBuilder(populateFilterBuilder(leftExpression, m), populateFilterBuilder(
                rightExpression, m));
    }

    /**
     * Gets the or filter builder.
     *
     * @param logicalExp the logical exp
     * @param m the m
     * @param entity the entity
     * @return the or filter builder
     */
    private OrFilterBuilder getOrFilterBuilder(Expression logicalExp, EntityMetadata m)
    {
        OrExpression orExp = (OrExpression) logicalExp;
        Expression leftExpression = orExp.getLeftExpression();
        Expression rightExpression = orExp.getRightExpression();

        return new OrFilterBuilder(populateFilterBuilder(leftExpression, m), populateFilterBuilder(
                rightExpression, m));
    }

    /**
     * Populate filter clause.
     *
     * @param conditionalExpression the conditional expression
     * @return the filter clause
     */
    private FilterClause populateFilterClause(ComparisonExpression conditionalExpression)
    {
        String property = ((StateFieldPathExpression) conditionalExpression.getLeftExpression()).getPath(1);
        String condition = conditionalExpression.getComparisonOperator();
        Expression rightExpression = conditionalExpression.getRightExpression();
        Object value = (rightExpression instanceof InputParameter) ? kunderaQuery.getParametersMap().get(
                (rightExpression).toParsedText()) : rightExpression.toParsedText();

        return (condition != null && property != null) ? kunderaQuery.new FilterClause(property, condition, value)
                : null;
    }
    
    /**
     * Gets the filter.
     *
     * @param clause the clause
     * @param metadata the metadata
     * @param entityType the entity type
     * @return the filter
     */
    private FilterBuilder getFilter(FilterClause clause, final EntityMetadata metadata)
    {
        String condition = clause.getCondition();
        Object value = clause.getValue().get(0);
        String name = ((AbstractAttribute) ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit())).entity(metadata.getEntityClazz()).getAttribute(clause.getProperty()))
                .getJPAColumnName();

        FilterBuilder filterBuilder = null;

        if (condition.equals("="))
        {
            filterBuilder = new TermFilterBuilder(name, value);
        }
        else if (condition.equals(">"))
        {
            filterBuilder = new RangeFilterBuilder(name).gt(value);
        }
        else if (condition.equals("<"))
        {
            filterBuilder = new RangeFilterBuilder(name).lt(value);
        }
        else if (condition.equals(">="))
        {
            filterBuilder = new RangeFilterBuilder(name).gte(value);
        }
        else if (condition.equals("<="))
        {
            filterBuilder = new RangeFilterBuilder(name).lte(value);
        }

        return filterBuilder;
    }
    
    /**
     * Gets the query.
     *
     * @param clause the clause
     * @param metadata the metadata
     * @return the query
     */
    private FilterBuilder getQueryBuilder(FilterClause clause, final EntityMetadata metadata)
    {
        String condition = clause.getCondition();
        String value = clause.getValue().get(0).toString();
        String name = ((AbstractAttribute) ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit())).entity(metadata.getEntityClazz()).getAttribute(clause.getProperty()))
                .getJPAColumnName();

        String likePattern = value.contains("%") ? value.replaceAll("%", "*") : "*" + value + "*";

        QueryBuilder queryBuilder = null;
        if (condition.equals(Expression.LIKE))
        {
            queryBuilder = QueryBuilders.wildcardQuery(name, likePattern);
        }

        return FilterBuilders.queryFilter(queryBuilder);
    }
}
