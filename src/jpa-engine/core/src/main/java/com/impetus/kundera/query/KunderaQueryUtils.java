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
package com.impetus.kundera.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.eclipse.persistence.jpa.jpql.parser.AbstractPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.AbstractSingleEncapsulatedExpression;
import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.BetweenExpression;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.DeleteStatement;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.IdentificationVariable;
import org.eclipse.persistence.jpa.jpql.parser.InExpression;
import org.eclipse.persistence.jpa.jpql.parser.InputParameter;
import org.eclipse.persistence.jpa.jpql.parser.JPQLExpression;
import org.eclipse.persistence.jpa.jpql.parser.KeywordExpression;
import org.eclipse.persistence.jpa.jpql.parser.LikeExpression;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.LowerExpression;
import org.eclipse.persistence.jpa.jpql.parser.NullComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.NumericLiteral;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.OrderByClause;
import org.eclipse.persistence.jpa.jpql.parser.OrderByItem;
import org.eclipse.persistence.jpa.jpql.parser.RegexpExpression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.StringLiteral;
import org.eclipse.persistence.jpa.jpql.parser.SubExpression;
import org.eclipse.persistence.jpa.jpql.parser.UpdateStatement;
import org.eclipse.persistence.jpa.jpql.parser.UpperExpression;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.ListIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * 
 * @author Amit Kumar
 */
public final class KunderaQueryUtils
{
    /** the log used by this class. */
    private static Logger logger = LoggerFactory.getLogger(KunderaQueryUtils.class);

    /**
     * Gets the where clause.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return the where clause
     */
    public static WhereClause getWhereClause(JPQLExpression jpqlExpression)
    {
        WhereClause whereClause = null;

        if (hasWhereClause(jpqlExpression))
        {
            if (isSelectStatement(jpqlExpression))
            {
                whereClause = (WhereClause) ((SelectStatement) jpqlExpression.getQueryStatement()).getWhereClause();

            }
            else if (isUpdateStatement(jpqlExpression))
            {
                whereClause = (WhereClause) ((UpdateStatement) jpqlExpression.getQueryStatement()).getWhereClause();

            }
            if (isDeleteStatement(jpqlExpression))
            {
                whereClause = (WhereClause) ((DeleteStatement) jpqlExpression.getQueryStatement()).getWhereClause();
            }
        }
        return whereClause;
    }

    public static List<String> getSelectColumns(JPQLExpression jpqlExpression)
    {
        List<String> columns;
        if (isSelectStatement(jpqlExpression))
        {
            columns = new ArrayList<>();
            SelectClause k = (SelectClause) ((SelectStatement) jpqlExpression.getQueryStatement()).getSelectClause();

            Expression selExp = k.getSelectExpression();

            if (selExp instanceof StateFieldPathExpression)
            {
                if (selExp.toActualText().indexOf(".") > 0)
                {
                    columns.add(selExp.toActualText().split("[.]")[1]);
                }
            }
            else if (selExp instanceof CollectionExpression)
            {
                CollectionExpression l = ((CollectionExpression) k.getSelectExpression());
                ListIterable<Expression> list = l.children();
                for (Expression exp : list)
                {
                    if (exp.toActualText().indexOf(".") > 0)
                    {
                        columns.add(exp.toActualText().split("[.]")[1]);
                    }
                }
            }
        }
        else
        {
            logger.error("Not a select Query");
            throw new KunderaException("Not a select Query");
        }
        return columns;
    }

    /**
     * Gets the order by clause.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return the order by clause
     */
    public static OrderByClause getOrderByClause(JPQLExpression jpqlExpression)
    {
        OrderByClause orderByClause = null;

        if (hasOrderBy(jpqlExpression))
        {
            orderByClause = (OrderByClause) ((SelectStatement) jpqlExpression.getQueryStatement()).getOrderByClause();
        }
        return orderByClause;
    }

    /**
     * Checks for where clause.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if successful
     */
    public static boolean hasWhereClause(JPQLExpression jpqlExpression)
    {
        if (isSelectStatement(jpqlExpression))
        {
            return ((SelectStatement) jpqlExpression.getQueryStatement()).hasWhereClause();
        }
        else if (isUpdateStatement(jpqlExpression))
        {
            return ((UpdateStatement) jpqlExpression.getQueryStatement()).hasWhereClause();
        }
        if (isDeleteStatement(jpqlExpression))
        {
            return ((DeleteStatement) jpqlExpression.getQueryStatement()).hasWhereClause();
        }
        return false;
    }

    /**
     * Checks for group by.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if successful
     */
    public static boolean hasGroupBy(JPQLExpression jpqlExpression)
    {
        if (isSelectStatement(jpqlExpression))
        {
            return ((SelectStatement) jpqlExpression.getQueryStatement()).hasGroupByClause();
        }
        return false;
    }

    /**
     * Checks for having.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if successful
     */
    public static boolean hasHaving(JPQLExpression jpqlExpression)
    {
        if (isSelectStatement(jpqlExpression))
        {
            return ((SelectStatement) jpqlExpression.getQueryStatement()).hasHavingClause();
        }
        return false;
    }

    /**
     * Checks for order by.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if successful
     */
    public static boolean hasOrderBy(JPQLExpression jpqlExpression)
    {
        if (isSelectStatement(jpqlExpression))
        {
            return ((SelectStatement) jpqlExpression.getQueryStatement()).hasOrderByClause();
        }
        return false;
    }

    /**
     * Checks if is select statement.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if is select statement
     */
    public static boolean isSelectStatement(JPQLExpression jpqlExpression)
    {
        return jpqlExpression.getQueryStatement().getClass().isAssignableFrom(SelectStatement.class);

    }

    /**
     * Checks if is delete statement.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if is delete statement
     */
    public static boolean isDeleteStatement(JPQLExpression jpqlExpression)
    {
        return jpqlExpression.getQueryStatement().getClass().isAssignableFrom(DeleteStatement.class);
    }

    /**
     * Checks if is update statement.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return true, if is update statement
     */
    public static boolean isUpdateStatement(JPQLExpression jpqlExpression)
    {
        return jpqlExpression.getQueryStatement().getClass().isAssignableFrom(UpdateStatement.class);
    }

    /**
     * Checks if is aggregated expression.
     * 
     * @param expression
     *            the expression
     * @return true, if is aggregated expression
     */
    public static boolean isAggregatedExpression(Expression expression)
    {
        return AggregateFunction.class.isAssignableFrom(expression.getClass());
    }

    /**
     * Gets the value.
     * 
     * @param exp
     *            the exp
     * @param clazz
     *            the clazz
     * @return the value
     */
    public static Object getValue(Expression exp, Class clazz, KunderaQuery kunderaQuery)
    {
        if (StringLiteral.class.isAssignableFrom(exp.getClass()))
        {
            return ((StringLiteral) exp).getUnquotedText();

        }
        else if (NumericLiteral.class.isAssignableFrom(exp.getClass()))
        {
            return PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz,
                    ((NumericLiteral) exp).getText());
        }
        else if (InputParameter.class.isAssignableFrom(exp.getClass()))
        {
            InputParameter ip = (InputParameter) exp;
            if (kunderaQuery.getParametersMap().containsKey(ip.getParameter()))
            {
                return kunderaQuery.getParametersMap().get(ip.getParameter());
            }
            else
            {
                return ip.getParameter();
            }
        }
        else if (KeywordExpression.class.isAssignableFrom(exp.getClass()))
        {
            KeywordExpression keyWordExp = (KeywordExpression) exp;
            return PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz,
                    keyWordExp.getActualIdentifier());
        }
        else if (IdentificationVariable.class.isAssignableFrom(exp.getClass()))
        {
            IdentificationVariable idvExp = (IdentificationVariable) exp;
            return idvExp.getText();
        }
        else
        {
            logger.warn("Arithmetic expression is not supported ");
            throw new KunderaException("Arithmetic expression is not supported currently");
        }
    }

    /**
     * Adds the to output columns.
     * 
     * @param selectExpression
     *            the select expression
     * @param m
     *            the m
     * @param columnsToOutput
     *            the columns to output
     */
    private static void addToOutputColumns(Expression selectExpression, EntityMetadata m,
            List<Map<String, Object>> columnsToOutput, KunderaMetadata kunderaMetadata)
    {

        Map<String, Object> map = setFieldClazzAndColumnFamily(selectExpression, m, kunderaMetadata);
        columnsToOutput.add(map);
    }

    /**
     * Read select clause.
     * 
     * @param selectExpression
     *            the select expression
     * @param m
     *            the m
     * @param useLuceneOrES
     *            the use lucene or es
     * @param kunderaMetadata
     * @return the list
     */
    public static List<Map<String, Object>> readSelectClause(Expression selectExpression, EntityMetadata m,
            Boolean useLuceneOrES, KunderaMetadata kunderaMetadata)
    {
        List<Map<String, Object>> columnsToOutput = new ArrayList<Map<String, Object>>();
        if (StateFieldPathExpression.class.isAssignableFrom(selectExpression.getClass()))
        {
            Expression sfpExp = selectExpression;

            addToOutputColumns(selectExpression, m, columnsToOutput, kunderaMetadata);
        }
        else if (CollectionExpression.class.isAssignableFrom(selectExpression.getClass()))
        {
            CollectionExpression collExp = (CollectionExpression) selectExpression;
            ListIterator<Expression> itr = collExp.children().iterator();
            while (itr.hasNext())
            {
                Expression exp = itr.next();
                if (StateFieldPathExpression.class.isAssignableFrom(exp.getClass()))
                {
                    addToOutputColumns(exp, m, columnsToOutput, kunderaMetadata);
                }
            }
        }
        return columnsToOutput;
    }

    /**
     * Sets the fieldclazz and colfamily.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @return the map
     */
    public static Map<String, Object> setFieldClazzAndColumnFamily(Expression expression, EntityMetadata m,
            final KunderaMetadata kunderaMetadata)
    {
        AbstractPathExpression pathExp = null;

        if (expression instanceof AbstractPathExpression) {
            pathExp = (AbstractPathExpression) expression;

        } else {
            if (expression instanceof AbstractSingleEncapsulatedExpression) {
                pathExp = (AbstractPathExpression) ((AbstractSingleEncapsulatedExpression) expression).getExpression();
            }

        }

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();
        Class fieldClazz = String.class;
        String colFamily = m.getTableName();
        String colName = null;
        Map<String, Object> map = new HashMap<String, Object>();

        boolean isEmbeddable = false;
        boolean isAssociation = false;
        int count = 1;
        String fieldName = pathExp.getPath(count++);

        AbstractAttribute attrib = (AbstractAttribute) entity.getAttribute(fieldName);
        String dbColName = attrib.getJPAColumnName();
        isEmbeddable = metaModel.isEmbeddable(attrib.getBindableJavaType());
        isAssociation = attrib.isAssociation();
        while (pathExp.pathSize() > count)
        {
            if (isEmbeddable)
            {
                EmbeddableType embeddableType = metaModel.embeddable(attrib.getBindableJavaType());
                String attName = pathExp.getPath(count++);
                fieldName = fieldName + "." + attName;
                attrib = (AbstractAttribute) embeddableType.getAttribute(attName);
                isEmbeddable = metaModel.isEmbeddable(attrib.getBindableJavaType());
                isAssociation = attrib.isAssociation();
                dbColName += ("." + attrib.getJPAColumnName());
            }
            else if (isAssociation)
            {
                String attName = pathExp.getPath(count++);
                fieldName = fieldName + "." + attName;
                EntityType associatedType = metaModel.entity(attrib.getBindableJavaType());
                attrib = (AbstractAttribute) associatedType.getAttribute(attName);
                isEmbeddable = metaModel.isEmbeddable(attrib.getBindableJavaType());
                isAssociation = attrib.isAssociation();
                dbColName += ("." + attrib.getJPAColumnName());
            }
            colName = fieldName;
        }

        if (!pathExp.getPath(count - 1).equals(discriminatorColumn))
        {
            fieldClazz = attrib.getBindableJavaType();
            colFamily = attrib.getTableName() != null ? attrib.getTableName() : m.getTableName();
            colName = colName != null ? colName : attrib.getJPAColumnName();

        }

        boolean ignoreCase =
              (expression instanceof UpperExpression) || (expression instanceof LowerExpression);

        map.put(Constants.FIELD_CLAZZ, fieldClazz);
        map.put(Constants.COL_FAMILY, colFamily);
        map.put(Constants.COL_NAME, colName);
        map.put(Constants.FIELD_NAME, fieldName);
        map.put(Constants.IS_EMBEDDABLE, isEmbeddable);
        map.put(Constants.DB_COL_NAME, dbColName);
        map.put(Constants.IGNORE_CASE, ignoreCase);
        return map;
    }

    /**
     * Traverse.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param idColumn
     *            the id column
     * @return the filter
     */
    public static void traverse(Expression expression, EntityMetadata m, KunderaMetadata kunderaMetadata,
            KunderaQuery kunderaQuery, boolean isSubExpression)
    {

        if (ComparisonExpression.class.isAssignableFrom(expression.getClass()))
        {
            onComparisonExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (NullComparisonExpression.class.isAssignableFrom(expression.getClass()))
        {
            onNullComparisonExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (LogicalExpression.class.isAssignableFrom(expression.getClass()))
        {
            onLogicalExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (InExpression.class.isAssignableFrom(expression.getClass()))
        {
            onInExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (LikeExpression.class.isAssignableFrom(expression.getClass()))
        {
            onLikeExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (RegexpExpression.class.isAssignableFrom(expression.getClass()))
        {
            onRegExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (BetweenExpression.class.isAssignableFrom(expression.getClass()))
        {
            onBetweenExpression(expression, m, kunderaMetadata, kunderaQuery);
        }
        else if (SubExpression.class.isAssignableFrom(expression.getClass()))
        {
            onSubExpression(expression, m, kunderaMetadata, kunderaQuery);
        }

    }

    /**
     * @param expression
     * @param m
     * @param kunderaMetadata
     * @param kunderaQuery
     */
    public static void onSubExpression(Expression expression, EntityMetadata m, KunderaMetadata kunderaMetadata,
            KunderaQuery kunderaQuery)
    {
        kunderaQuery.addFilterClause("(");
        traverse(((SubExpression) expression).getExpression(), m, kunderaMetadata, kunderaQuery, true);
        kunderaQuery.addFilterClause(")");

    }

    /**
     * @param expression
     * @param m
     * @param kunderaMetadata
     * @param kunderaQuery
     * @return
     */
    public static Map<String, Object> onBetweenExpression(Expression expression, EntityMetadata m,
            KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        BetweenExpression betweenExp = (BetweenExpression) expression;
        Expression sfpExp = betweenExp.getExpression();

        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        String columnName = (String) map.get(Constants.COL_NAME);
        String fieldName = (String) map.get(Constants.FIELD_NAME);
        kunderaQuery.addFilterClause(
              columnName, Expression.GREATER_THAN_OR_EQUAL, betweenExp.getLowerBoundExpression().toActualText(),
              fieldName, (Boolean) map.get(Constants.IGNORE_CASE));
        kunderaQuery.addFilterClause("AND");
        kunderaQuery.addFilterClause(
              columnName, Expression.LOWER_THAN_OR_EQUAL, betweenExp.getUpperBoundExpression().toActualText(),
              fieldName, (Boolean) map.get(Constants.IGNORE_CASE));

        return map;

    }

    /**
     * @param expression
     * @param m
     * @param kunderaMetadata
     * @param kunderaQuery
     * @return
     */
    public static Map<String, Object> onLikeExpression(Expression expression, EntityMetadata m,
            KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        LikeExpression likeExp = (LikeExpression) expression;
        Expression sfpExp = likeExp.getStringExpression();
        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        kunderaQuery.addFilterClause(
              (String) map.get(Constants.COL_NAME), likeExp.getIdentifier(), likeExp.getPatternValue().toActualText(),
              (String) map.get(Constants.FIELD_NAME), (Boolean) map.get(Constants.IGNORE_CASE));
        return map;

    }

    /**
     * On reg expression.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param kunderaMetadata
     *            the kundera metadata
     * @param kunderaQuery
     *            the kundera query
     * @return the map
     */
    public static Map<String, Object> onRegExpression(Expression expression, EntityMetadata m,
            KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        RegexpExpression regExp = (RegexpExpression) expression;
        Expression sfpExp = regExp.getStringExpression();
        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        kunderaQuery.addFilterClause(
              (String) map.get(Constants.COL_NAME), regExp.getActualRegexpIdentifier().toUpperCase(),
              regExp.getPatternValue().toActualText(), (String) map.get(Constants.FIELD_NAME),
              (Boolean) map.get(Constants.IGNORE_CASE));
        return map;
    }

    /**
     * On logical expression.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param idColumn
     *            the id column
     * @return the filter
     */
    public static void onLogicalExpression(Expression expression, EntityMetadata m, KunderaMetadata kunderaMetadata,
            KunderaQuery kunderaQuery)
    {
        if (expression instanceof OrExpression)
        {
            kunderaQuery.addFilterClause("(");
        }

        traverse(((LogicalExpression) expression).getLeftExpression(), m, kunderaMetadata, kunderaQuery, false);

        if (expression instanceof OrExpression)
        {
            kunderaQuery.addFilterClause(")");
        }

        kunderaQuery.addFilterClause(((LogicalExpression) expression).getIdentifier());

        if (expression instanceof OrExpression)
        {
            kunderaQuery.addFilterClause("(");
        }

        traverse(((LogicalExpression) expression).getRightExpression(), m, kunderaMetadata, kunderaQuery, false);

        if (expression instanceof OrExpression)
        {
            kunderaQuery.addFilterClause(")");
        }
    }

    /**
     * On in expression.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param idColumn
     *            the id column
     * @param isIdColumn
     *            the is id column
     * @return
     * @return the filter
     */
    public static Map<String, Object> onInExpression(Expression expression, EntityMetadata m,
            KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        InExpression inExp = (InExpression) expression;
        Expression sfpExp = inExp.getExpression();
        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        kunderaQuery.addFilterClause(
              (String) map.get(Constants.COL_NAME), inExp.getIdentifier(), inExp.getInItems(),
              (String) map.get(Constants.FIELD_NAME), (Boolean) map.get(Constants.IGNORE_CASE));
        return map;
    }

    /**
     * On comparison expression.
     * 
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param idColumn
     *            the id column
     * @param isIdColumn
     *            the is id column
     * @return
     * @return the filter
     */
    public static Map<String, Object> onComparisonExpression(Expression expression, EntityMetadata m,
            KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        ComparisonExpression compExp = (ComparisonExpression) expression;

        String condition = compExp.getIdentifier();
        Expression sfpExp = compExp.getLeftExpression();
        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        Object value = KunderaQueryUtils.getValue(compExp.getRightExpression(), (Class) map.get(Constants.FIELD_CLAZZ),
                kunderaQuery);
        kunderaQuery.addFilterClause(
              (String) map.get(Constants.COL_NAME), condition, value,
              (String) map.get(Constants.FIELD_NAME), (Boolean) map.get(Constants.IGNORE_CASE));
        return map;

    }

    /**
     * On null-comparison expression.
     *
     * @param expression
     *            the expression
     * @param m
     *            the m
     * @param idColumn
     *            the id column
     * @param isIdColumn
     *            the is id column
     * @return
     * @return the filter
     */
    public static Map<String, Object> onNullComparisonExpression(Expression expression, EntityMetadata m,
                                                                 KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery)
    {
        NullComparisonExpression compExp = (NullComparisonExpression) expression;

        String condition = compExp.getIdentifier();
        Expression sfpExp = compExp.getExpression();
        Map<String, Object> map = KunderaQueryUtils.setFieldClazzAndColumnFamily(sfpExp, m, kunderaMetadata);
        kunderaQuery.addFilterClause(
              (String) map.get(Constants.COL_NAME), condition, null,
              (String) map.get(Constants.FIELD_NAME), (Boolean) map.get(Constants.IGNORE_CASE));
        return map;

    }

    /**
     * Gets the order by items.
     * 
     * @param jpqlExpression
     *            the jpql expression
     * @return the order by items
     */
    public static List<OrderByItem> getOrderByItems(JPQLExpression jpqlExpression)
    {
        List<OrderByItem> orderList = new LinkedList<>();

        if (hasOrderBy(jpqlExpression))
        {
            Expression orderByItems = getOrderByClause(jpqlExpression).getOrderByItems();

            if (orderByItems instanceof CollectionExpression)
            {
                ListIterator<Expression> iterator = orderByItems.children().iterator();
                while (iterator.hasNext())
                {
                    OrderByItem orderByItem = (OrderByItem) iterator.next();
                    orderList.add(orderByItem);
                }
            }
            else
            {
                orderList.add((OrderByItem) orderByItems);
            }
        }

        return orderList;
    }
}
