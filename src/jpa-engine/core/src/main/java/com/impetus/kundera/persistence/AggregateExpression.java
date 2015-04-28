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
package com.impetus.kundera.persistence;

import javax.persistence.criteria.Expression;

/**
 * The Class AggregateExpression.
 * 
 * @author: karthikp.manchala
 */
public class AggregateExpression extends AbstractExpression<Long>
{

    /** The expression. */
    private Expression<?> expression;

    /** The aggregation. */
    private String aggregation;

    /**
     * Instantiates a new aggregate expression.
     * 
     * @param expression
     *            the expression
     * @param aggregation
     *            the aggregation
     */
    public AggregateExpression(Expression<?> expression, String aggregation)
    {
        this.expression = expression;
        this.aggregation = aggregation;
    }

    /**
     * Gets the expression.
     * 
     * @return the expression
     */
    public Expression<?> getExpression()
    {
        return expression;
    }

    /**
     * Sets the expression.
     * 
     * @param expression
     *            the new expression
     */
    public void setExpression(Expression<?> expression)
    {
        this.expression = expression;
    }

    /**
     * Gets the aggregation.
     * 
     * @return the aggregation
     */
    public String getAggregation()
    {
        return aggregation;
    }

    /**
     * Sets the aggregation.
     * 
     * @param aggregation
     *            the new aggregation
     */
    public void setAggregation(String aggregation)
    {
        this.aggregation = aggregation;
    }

}
