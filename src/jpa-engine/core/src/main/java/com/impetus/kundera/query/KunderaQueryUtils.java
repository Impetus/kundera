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

import org.eclipse.persistence.jpa.jpql.parser.DeleteStatement;
import org.eclipse.persistence.jpa.jpql.parser.JPQLExpression;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.UpdateStatement;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;

/**
 * 
 * @author Amit Kumar
 */
public final class KunderaQueryUtils
{
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
}
