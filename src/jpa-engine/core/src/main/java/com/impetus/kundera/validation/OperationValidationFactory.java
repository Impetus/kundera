/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.validation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.impetus.kundera.validation.rules.IRule;
import com.impetus.kundera.validation.rules.RuleValidationException;

/**
 * @author Chhavi Gangwal
 *
 */
public class OperationValidationFactory extends AbstractValidationFactory implements ValidationFactory
{
    /**
     * List of rules for operation level validations
     */
    static List<IRule> rules = new ArrayList<IRule>();

    static
    {
        
       // rules.add((IRule) new AttributeConstraintRule());
        
    }
    
    /**
     * Constructor
     * adds static rule list to Rule factory
     */
    public OperationValidationFactory()
    {
        this.ruleFactory.addRule(rules);
    }
    
    
    /* (non-Javadoc)
     * @see com.impetus.kundera.validation.AbstractValidationFactory#validate(java.lang.reflect.Field, java.lang.Object, com.impetus.kundera.validation.rules.IRule[])
     */
    @Override
    public boolean validate(Field field, Object fieldValue, IRule... rules) throws RuleValidationException
    {

        if (rules == null)
        {
            return super.validate(field, fieldValue, this.ruleFactory.getJpaRules());
        }
        
        else
        {
            return super.validate(field, fieldValue, rules);
        }

    }

   
}
