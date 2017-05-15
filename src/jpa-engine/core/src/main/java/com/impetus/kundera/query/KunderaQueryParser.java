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

import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.FromClause;
import org.eclipse.persistence.jpa.jpql.parser.GroupByClause;
import org.eclipse.persistence.jpa.jpql.parser.HavingClause;
import org.eclipse.persistence.jpa.jpql.parser.OrderByClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser for handling JPQL Single-String queries. Takes a JPQLQuery and the
 * query string and parses it into its constituent parts, updating the JPQLQuery
 * accordingly with the result that after calling the parse() method the
 * JPQLQuery is populated.
 * 
 * <pre>
 * SELECT [ {result} ]
 * [FROM {candidate-classes} ]
 * [WHERE {filter}]
 * [GROUP BY {grouping-clause} ]
 * [HAVING {having-clause} ]
 * [ORDER BY {ordering-clause}]
 * e.g SELECT c FROM Customer c INNER JOIN c.orders o WHERE c.status = 1
 * </pre>
 * 
 * @author animesh.kumar
 */
public class KunderaQueryParser
{

    /** The JPQL query to populate. */
    private KunderaQuery query;

    /** The single-string query string. */
    private String queryString;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaQueryParser.class);

    /**
     * Constructor for the Single-String parser.
     * 
     * @param query
     *            The query
     * @param queryString
     *            The Single-String query
     */
    public KunderaQueryParser(KunderaQuery query)
    {
        this.query = query;
        this.queryString = query.getJPAQuery();
    }

    /**
     * Method to parse the Single-String query.
     */
    public final void parse()
    {
        new Compiler().compile();
    }

    /**
     * Method to detect whether this token is a keyword for JPQL Single-String.
     * 
     * @param token
     *            The token
     * 
     * @return Whether it is a keyword
     */
    private boolean isKeyword(String token)
    {
        // Compare the passed token against the provided keyword list, or their
        // lowercase form
        for (int i = 0; i < KunderaQuery.SINGLE_STRING_KEYWORDS.length; i++)
        {
            if (token.equalsIgnoreCase(KunderaQuery.SINGLE_STRING_KEYWORDS[i]))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Compiler to process keywords contents. In the query the keywords often
     * have content values following them that represent the constituent parts
     * of the query. This takes the keyword and sets the constituent part
     * accordingly.
     */
    private class Compiler
    {

        // Temporary variable since grouping clause is made up of GROUP BY ...
        // HAVING ...
        /** The grouping clause. */
        private String groupingClause;

        /**
         * Compile.
         */
        private void compile()
        {
            // if it is not an update statement
            if (!compileUpdate())
            {
                compileSelectOrDelete();
            }
        }

        /**
         * Compile update.
         * 
         * @return true, if successful
         */
        private boolean compileUpdate()
        {
            if (query.isUpdateStatement())
            {
                query.setIsDeleteUpdate(true);
                compileFrom();
                compileUpdateClause();

                compilewhereClause();

                return true;
            }
            else
            {
                // initiateExpressionFactory("SELECT");
            }
            return false;
        }

        /**
         * Compile select.
         */
        private void compileSelectOrDelete()
        {

            if (!query.isSelectStatement())
            {
                if (query.isDeleteStatement())
                {
                    query.setIsDeleteUpdate(true);

                }
            }
            else
            {
                // initiateExpressionFactory("SELECT");
            }

            compileFrom();
            compileResult();

            compilewhereClause();
        }

        private boolean isWhereClause()
        {
            if (query.isSelectStatement())
            {
                return query.getSelectStatement().hasWhereClause();
            }
            else if (query.isUpdateStatement())
            {
                return query.getUpdateStatement().hasWhereClause();
            }
            if (query.isDeleteStatement())
            {
                return query.getDeleteStatement().hasWhereClause();
            }
            return false;
        }

        private boolean isGroupBy()
        {
            if (query.isSelectStatement())
            {
                return query.getSelectStatement().hasGroupByClause();
            }
            return false;
        }

        private boolean isHaving()
        {
            if (query.isSelectStatement())
            {
                return query.getSelectStatement().hasHavingClause();
            }
            return false;
        }

        private boolean isOrderBy()
        {
            if (query.isSelectStatement())
            {
                return query.getSelectStatement().hasOrderByClause();
            }
            return false;
        }

        /**
         * Compilewhere clause.
         */
        private void compilewhereClause()
        {

            if (isWhereClause())
            {
                compileWhere();
            }
            if (isGroupBy())
            {
                compileGroup();
            }
            if (isHaving())
            {
                compileHaving();
            }
            if (groupingClause != null)
            {
                query.setGrouping(groupingClause);
            }

            if (isOrderBy())
            {
                compileOrder();
            }
        }

        /**
         * Compile result.
         */
        private void compileResult()
        {
            buildResultColumns();
        }

        /**
         * Compile from.
         */
        private void compileFrom()
        {
            buildFrom();
        }

        /**
         * Compile from.
         */
        private void compileUpdateClause()
        {
            ListIterator<Expression> updateColumnIter = null;
            if (query.isUpdateStatement() && query.getUpdateStatement().getUpdateClause().hasUpdateItems())
            {

                if (query.getUpdateStatement().getUpdateClause().getUpdateItems() instanceof CollectionExpression)
                {

                    updateColumnIter = ((CollectionExpression) query.getUpdateStatement().getUpdateClause()
                            .getUpdateItems()).children().iterator();
                    while (updateColumnIter.hasNext())
                    {
                        ListIterator<Expression> childUpdateClauseIter = updateColumnIter.next().children().iterator();

                        while (childUpdateClauseIter.hasNext())
                        {
                            addUpdateClause(childUpdateClauseIter);

                        }

                    }
                }
                else
                {

                    updateColumnIter = query.getUpdateStatement().getUpdateClause().getUpdateItems().children()
                            .iterator();
                    addUpdateClause(updateColumnIter);

                }

            }
        }

        private void addUpdateClause(ListIterator<Expression> childUpdateClauseIter)
        {
            String columnTuple = childUpdateClauseIter.next().toActualText().trim();
            String value = childUpdateClauseIter.next().toActualText().trim();
            StringTokenizer tokenizer = new StringTokenizer(columnTuple, ".");
            columnTuple = getTokenizedValue(tokenizer);

            query.addUpdateClause(columnTuple, value);
        }

        private String getTokenizedValue(StringTokenizer tokenizer)
        {
            String value = null;

            while (tokenizer.hasMoreTokens())
            {
                value = tokenizer.nextToken();
            }
            return value;
        }

        /**
         * Compile where.
         */
        private void compileWhere()
        {
            WhereClause whereClause = null;

            if (query.isSelectStatement())
            {
                whereClause = (WhereClause) query.getSelectStatement().getWhereClause();

            }
            else if (query.isUpdateStatement())
            {
                whereClause = (WhereClause) query.getUpdateStatement().getWhereClause();

            }
            if (query.isDeleteStatement())
            {
                whereClause = (WhereClause) query.getDeleteStatement().getWhereClause();

            }

            String content = whereClause.getConditionalExpression().toActualText();
            if (whereClause != null && content.length() == 0)
            {
                throw new JPQLParseException("keyword without value[WHERE]");
            }
            query.setFilter(content);
        }

        /**
         * Compile group.
         */
        private void compileGroup()
        {
            GroupByClause groupByClause = null;

            if (query.isSelectStatement())
            {
                groupByClause = (GroupByClause) query.getSelectStatement().getGroupByClause();

            }

            // content cannot be empty
            if (groupByClause == null || groupByClause.toActualText().length() == 0)
            {
                throw new JPQLParseException("keyword without value: GROUP BY");
            }
            groupingClause = groupByClause.toActualText();
        }

        /**
         * Compile having.
         */
        private void compileHaving()
        {
            HavingClause havingClause = null;

            if (query.isSelectStatement())
            {
                havingClause = (HavingClause) query.getSelectStatement().getHavingClause();

            }

            // content cannot be empty
            if (havingClause == null || havingClause.toActualText().length() == 0)
            {
                throw new JPQLParseException("keyword without value: HAVING");
            }

            if (groupingClause != null)
            {
                groupingClause = groupingClause.trim() + havingClause.toActualText();
            }
            else
            {
                groupingClause = havingClause.toActualText();
            }
        }

        /**
         * Compile order.
         */
        private void compileOrder()
        {
            OrderByClause orderByClause = null;

            if (query.isSelectStatement())
            {
                orderByClause = (OrderByClause) query.getSelectStatement().getOrderByClause();

            }
            if (orderByClause == null || !orderByClause.hasOrderByItems())
            {
                throw new JPQLParseException("keyword without value: ORDER BY");
            }
            query.setOrdering(orderByClause.getOrderByItems().toActualText());
        }
    }

    private void buildResultColumns()
    {
        String[] result = null, aggResult = null;
        if (query.isSelectStatement())
        {
            SelectClause selectClause = (SelectClause) (query.getSelectStatement().getSelectClause());
            ListIterator<Expression> selectColumnIter = null;

            int size = 0;
            if (selectClause.getSelectExpression() instanceof CollectionExpression)
            {
                CollectionExpression selectColumnExpression = (CollectionExpression) selectClause.getSelectExpression();
                selectColumnIter = selectColumnExpression.children().iterator();
                size = selectColumnExpression.childrenSize();

            }
            else
            {
                if (selectClause.getSelectExpression().toActualText().indexOf(".") > 0)
                {
                    size = 1;
                }
            }

            int aggregationCount = countAggregation(selectClause.getSelectExpression());
            query.setAggregated(aggregationCount > 0 || query.getSelectStatement().hasGroupByClause());
            int count = 0, aggCounter = 0, resultSize = size + 1 - aggregationCount;
            if (resultSize == 0)
            {
                resultSize = 1;
            }
            result = new String[resultSize];
            aggResult = new String[aggregationCount + 1];
            // content may be empty
            if (selectColumnIter != null)
            {
                while (selectColumnIter.hasNext())
                {
                    Expression nextExpression = selectColumnIter.next();
                    String property = nextExpression.toActualText();
                    if (isAggregation(nextExpression))
                    {
                        aggCounter = buildResult(aggResult, aggCounter,
                                property.substring(property.indexOf('(') + 1, property.indexOf(')')));
                    }
                    else
                        count = buildResult(result, count, property);
                }
            }
            else
            {
                String property = selectClause.getSelectExpression().toActualText();
                if (isAggregation(selectClause.getSelectExpression()))
                    aggCounter = buildResult(aggResult, aggCounter,
                            property.substring(property.indexOf('(') + 1, property.indexOf(')')));
                else
                    count = buildResult(result, count, property);
            }
            result[0] = result[0] == null ? aggResult[0] : result[0];
            aggResult[0] = aggResult[0] == null ? result[0] : aggResult[0];

            query.setResult(result);
            query.setAggregationResult(aggResult);
        }
    }

    /**
     * @param selectExpression
     * @return Count of aggregation required
     */
    private int countAggregation(Expression selectExpression)
    {
        int count = 0;
        if (selectExpression instanceof CollectionExpression)
        {
            CollectionExpression selectColumnExpression = (CollectionExpression) selectExpression;
            ListIterator<Expression> selectColumnIter = selectColumnExpression.children().iterator();

            while (selectColumnIter.hasNext())
            {
                count = isAggregation(selectColumnIter.next()) ? ++count : count;
            }
        }
        else
        {
            if (isAggregation(selectExpression))
                count = 1;
        }
        return count;
    }

    private boolean isAggregation(Expression expression)
    {
        return expression instanceof AggregateFunction;
    }

    private int buildResult(String[] result, int count, String property)
    {
        if (property != null && property.length() > 0)
        {
            if (property.indexOf(".") > 0)
            {
                result[0] = property.substring(0, property.indexOf("."));
                String fieldName = property.substring(property.indexOf(".") + 1, property.length());
                if (fieldName == null || fieldName.isEmpty())
                {
                    throw new JPQLParseException(
                            "You have not given any column name after . ,Column name should not be empty");
                }
                if (result[count] == null)
                {
                    throw new JPQLParseException("Bad query format");
                }
                result[++count] = fieldName;
            }
            else
            {
                if (count > 0 && !query.isAggregated())
                {
                    throw new JPQLParseException("Bad query format");
                }
                result[count] = property;
                count++;
            }
        }
        return count;
    }

    private void buildFrom()
    {
        ListIterator<Expression> fromIter = null;
        if (query.isSelectStatement() && query.getSelectStatement().hasFromClause())
        {
            FromClause fromClause = (FromClause) query.getSelectStatement().getFromClause();
            fromIter = fromClause.children().iterator();

        }
        else if (query.isUpdateStatement())
        {
            fromIter = query.getUpdateStatement().getUpdateClause().children().iterator();

        }
        if (query.isDeleteStatement())
        {
            fromIter = query.getDeleteStatement().getDeleteClause().children().iterator();

        }

        if (fromIter != null)
        {
            while (fromIter.hasNext())
            {
                String textObj = fromIter.next().toActualText().trim();

                if (!StringUtils.isEmpty(textObj))
                {
                    query.setFrom(textObj);
                    break;
                }

            }
        }
    }

}