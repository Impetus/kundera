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
import java.util.Arrays;
import java.util.List;

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;

/**
 * Implementation class for AND {@link Predicate}
 * @author vivek.mishra
 *
 */
public class ConjuctionPredicate extends AbstractPredicate 
{

    private List<Expression<Boolean>> expressions = new ArrayList<Expression<Boolean>>();
    
    ConjuctionPredicate()
    {
        
    }
    
    ConjuctionPredicate(Expression<Boolean>...paramArrayOfExpression)
    {
        this.expressions = Arrays.asList(paramArrayOfExpression);
    }
    
    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#getOperator()
     */
    @Override
    public BooleanOperator getOperator()
    {
        return BooleanOperator.AND;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Predicate#getExpressions()
     */
    @Override
    public List<Expression<Boolean>> getExpressions()
    {
        return this.expressions;
    }

}
