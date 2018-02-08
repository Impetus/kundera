/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.criteria.CompoundSelection;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.metamodel.Attribute;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.AbstractPredicate.ConditionalOperator;
import com.impetus.kundera.query.KunderaQuery.SortOrder;

/**
 * Translator class to transform {@link CriteriaQuery} to JPQL.
 * 
 * @author vivek.mishra
 */
final class CriteriaQueryTranslator
{

    static Map<ConditionalOperator, String> conditions = new HashMap<AbstractPredicate.ConditionalOperator, String>();

    static
    {
        conditions.put(ConditionalOperator.EQ, "=");
        conditions.put(ConditionalOperator.LT, "<");
        conditions.put(ConditionalOperator.LTE, "<=");
        conditions.put(ConditionalOperator.GT, ">");
        conditions.put(ConditionalOperator.GTE, ">=");
        conditions.put(ConditionalOperator.BTW, "BETWEEN");
    }

    /**
     * Method to translate criteriaQuery into JPQL.
     * 
     * @param criteriaQuery
     *            criteria query.
     * 
     * @return JPQL string.
     */
    static <S> String translate(CriteriaQuery criteriaQuery)
    {
        QueryBuilder builder = new CriteriaQueryTranslator.QueryBuilder();

        // validate if criteria query is valid

        /**
         * select, from clause is mandatory
         * 
         * multiple from clause not support where clause is optional
         * 
         */

        Selection<S> select = criteriaQuery.getSelection();

        if (select != null)
        {
            builder.appendSelectClause();

        }

        if (select.getClass().isAssignableFrom(DefaultCompoundSelection.class)
                && ((CompoundSelection) select).isCompoundSelection())
        {
            List<Selection<?>> selections = ((CompoundSelection) select).getCompoundSelectionItems();
            builder.appendMultiSelect(selections);
        }
        else if (select instanceof AggregateExpression)
        {
            builder.appendAggregate(((AggregateExpression) select).getAggregation());
        }
        else
        {
            String alias = select.getAlias();

            if (!StringUtils.isEmpty(alias))
            {
                builder.appendAlias(alias);
            }

            Attribute attribute = ((DefaultPath) select).getAttribute();

            if (attribute != null)
            {
                builder.appendAttribute(attribute);
            }
        }
        Class<? extends S> clazzType = select.getJavaType();

        Set<Root<?>> roots = criteriaQuery.getRoots();

        Root<?> from = roots.iterator().next();

        Class entityClazz = from.getJavaType();

        builder.appendFromClause();

        // select.alias(paramString)
        builder.appendFrom(entityClazz);
        builder.appendAlias(from.getAlias() != null ? from.getAlias() : select.getAlias());
        Predicate where = criteriaQuery.getRestriction(); // this could be null.
        if (where != null)
        {
            builder.appendWhereClause();
            List<Expression<Boolean>> expressions = where.getExpressions();
            for (Expression expr : expressions)
            {
                builder.appendWhere(expr, from.getAlias());
            }

        }

        List<Order> orderings = criteriaQuery.getOrderList();

        if (orderings != null && !orderings.isEmpty())
        {
            builder.appendOrderClause(where == null);
            String alias = from.getAlias() != null ? from.getAlias() : select.getAlias();
            builder.appendMultiOrdering(orderings, alias);
        }
        return builder.getQuery();

        // check that roots has to be one. multiple clause not yet supported

    }

    /**
     * @author vivek.mishra QueryBuilder class
     */
    static class QueryBuilder
    {

        private StringBuilder builder = new StringBuilder();

        QueryBuilder()
        {

        }

        QueryBuilder appendAggregate(String aggregation)
        {
            this.builder.append(aggregation);
            return this;

        }

        public String getQuery()
        {
            return this.builder.toString();
        }

        QueryBuilder appendSelectClause()
        {
            this.builder.append("Select");
            this.builder.append(Constants.SPACE);
            return this;
        }

        QueryBuilder appendAlias(final String alias)
        {
            this.builder.append(alias);
            return this;
        }

        QueryBuilder appendOrderClause(boolean prefixSpace)
        {
            if (prefixSpace)
            {
                this.builder.append(Constants.SPACE);
            }
            this.builder.append("ORDER BY");
            this.builder.append(Constants.SPACE);
            return this;

        }
        
        QueryBuilder appendMultiOrdering(List<Order> orderings, String alias)
        {
            for (Order order : orderings)
            {
                this.appendAlias(alias);
                this.appendOrdering(order);
                this.builder.append(Constants.COMMA);
            }

            // remove last comma
            this.builder.deleteCharAt(this.builder.length() - 1);
            return this;
        }

        QueryBuilder appendOrdering(Order orderAttribute)
        {
            DefaultPath expression = (DefaultPath) orderAttribute.getExpression();
            Attribute attrib = expression.getAttribute();
            Attribute embedAttribute = expression.getEmbeddedAttribute();
            String fieldName = null;

            this.builder.append(".");
            if (embedAttribute != null)
            {
                fieldName = embedAttribute.getName();
                this.builder.append(fieldName);
                this.builder.append(".");
            }

            this.builder.append(attrib.getName());

            SortOrder order = ((QueryOrder) orderAttribute).getOrder();

            this.builder.append(Constants.SPACE);
            this.builder.append(order.name());
            return this;
        }

        private QueryBuilder appendMultiSelectSuffix()
        {
            this.builder.append(",");
            return this;
        }

        QueryBuilder appendMultiSelect(List<Selection<?>> selections)
        {
            for (Selection s : selections)
            {
                Attribute attribute = ((DefaultPath) s).getAttribute();

                if (attribute != null)
                {
                    this.builder.append(s.getAlias());
                    this.appendAttribute(attribute);
                    this.appendMultiSelectSuffix();
                }

            }
            this.builder.delete(this.builder.toString().lastIndexOf(","), this.builder.length());
            return this;
        }

        QueryBuilder appendFromClause()
        {
            this.builder.append(Constants.SPACE);
            this.builder.append("from");
            this.builder.append(Constants.SPACE);
            return this;
        }

        QueryBuilder appendFrom(final Class entityClazz)
        {
            this.builder.append(entityClazz.getSimpleName());
            this.builder.append(Constants.SPACE);
            return this;
        }

        /**
         * @return
         */
        QueryBuilder appendWhereClause()
        {
            this.builder.append(Constants.SPACE);
            this.builder.append("where");
            this.builder.append(Constants.SPACE);
            return this;
        }

        /**
         * @param attrib
         * @return
         */
        QueryBuilder appendAttribute(Attribute attrib)
        {
            this.builder.append(".");
            this.builder.append(attrib.getName());
            return this;
        }

        /**
         * @param expr
         * @param alias
         * @return
         */
        QueryBuilder appendWhere(final Expression<Boolean> expr, final String alias)
        {

            if (expr.getClass().isAssignableFrom(ComparisonPredicate.class))
            {
                appendValueClause(alias, expr);
            }
            else
            {
                List<Expression<Boolean>> exprs = new ArrayList<Expression<Boolean>>();
                if (expr.getClass().isAssignableFrom(ConjuctionPredicate.class))
                {
                    exprs = ((ConjuctionPredicate) expr).getExpressions();
                    for (Expression<Boolean> e : exprs)
                    {
                        appendWhere(e, alias);
                        this.builder.append("AND");
                        this.builder.append(Constants.SPACE);
                    }

                    this.builder.delete(this.builder.toString().lastIndexOf("AND"), this.builder.length());
                }
                else if (expr.getClass().isAssignableFrom(DisjunctionPredicate.class))
                {
                    exprs = ((DisjunctionPredicate) expr).getExpressions();
                    for (Expression<Boolean> e : exprs)
                    {
                        appendWhere(e, alias);
                        this.builder.append("OR");
                        this.builder.append(Constants.SPACE);
                    }
                    this.builder.delete(this.builder.toString().lastIndexOf("OR"), this.builder.length());
                }
                else if (expr.getClass().isAssignableFrom(BetweenPredicate.class))
                {
                    Expression btExpression = ((BetweenPredicate) expr).getExpression();
                    appendBTValueClause(alias, btExpression, (BetweenPredicate) expr);
                }

            }

            return this;
        }

        /**
         * @param alias
         * @param expr
         */
        private void appendValueClause(final String alias, Expression expr)
        {
            ConditionalOperator condition = ((ComparisonPredicate) expr).getCondition();
            DefaultPath path = (DefaultPath) ((ComparisonPredicate) expr).getLhs();
            Object value = ((ComparisonPredicate) expr).getRhs();
            this.builder.append(alias);
            this.builder.append(".");
            if (path.getEmbeddedAttribute() != null)
            {
                this.builder.append(path.getEmbeddedAttribute().getName());
                this.builder.append(".");
            }
            this.builder.append(path.getAttribute().getName());
            this.builder.append(Constants.SPACE);
            this.builder.append(conditions.get(condition));
            this.builder.append(Constants.SPACE);
            appendValue(value, isStringLiteral(((AbstractAttribute) path.getAttribute()).getBindableJavaType(), value));
            this.builder.append(Constants.SPACE);
        }

        /**
         * @param alias
         * @param expr
         * @param btw
         */
        private void appendBTValueClause(final String alias, Expression expr, BetweenPredicate btw)
        {
            DefaultPath path = (DefaultPath) expr;
            this.builder.append(alias);
            this.builder.append(".");
            this.builder.append(path.getAttribute().getName());
            this.builder.append(Constants.SPACE);
            this.builder.append(conditions.get(btw.getCondition()));
            this.builder.append(Constants.SPACE);
            appendValue(btw.getLower(),
                    isStringLiteral(((AbstractAttribute) path.getAttribute()).getBindableJavaType(), btw.getLower()));
            this.builder.append(Constants.SPACE);
            this.builder.append("AND");
            this.builder.append(Constants.SPACE);
            appendValue(btw.getUpper(),
                    isStringLiteral(((AbstractAttribute) path.getAttribute()).getBindableJavaType(), btw.getUpper()));
        }

        /**
         * @param value
         * @param isString
         */
        private void appendValue(Object value, boolean isString)
        {
            if (isString)
            {
                this.builder.append("\"").append(value).append("\"");
            }
            else
            {
                this.builder.append(value);
            }

        }

        /**
         * @param fieldClazz
         * @param value
         * @return
         */
        private boolean isStringLiteral(Class fieldClazz, Object value)
        {
            return !isNumericValue(fieldClazz) && !isNumericValue(value.getClass());
        }

        /**
         * @param clazz
         * @return
         */
        private boolean isNumericValue(Class clazz)
        {

            if (!clazz.isPrimitive())
            {
                return Number.class.isAssignableFrom(clazz.getSuperclass());
            }
            else
            {
                return int.class.isAssignableFrom(clazz) || float.class.isAssignableFrom(clazz)
                        || double.class.isAssignableFrom(clazz) || long.class.isAssignableFrom(clazz)
                        || byte.class.isAssignableFrom(clazz) || short.class.isAssignableFrom(clazz);
            }

        }

    }

}
