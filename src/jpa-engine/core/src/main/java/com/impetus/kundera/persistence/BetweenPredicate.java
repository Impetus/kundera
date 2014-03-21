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

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;

/**
 * Implementation class for between clause {@link Predicate}
 * 
 * @author vivek.mishra
 * 
 */
public final class BetweenPredicate extends AbstractPredicate
{
    private Expression<?> expression;

    private Object lower;

    private Object upper;

    private ConditionalOperator condition = ConditionalOperator.BTW;

    BetweenPredicate(Expression expr, Object lower, Object upper)
    {
        this.expression = expr;
        this.lower = lower;
        this.upper = upper;
    }

    ConditionalOperator getCondition()
    {
        return this.condition;
    }

    Object getUpper()
    {
        return this.upper;
    }

    Object getLower()
    {
        return this.lower;
    }

    Expression getExpression()
    {
        return this.expression;
    }
}
