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

import java.util.ArrayList;
import java.util.List;

import com.impetus.kundera.validation.rules.EntityAnnotationRule;
import com.impetus.kundera.validation.rules.EntityFieldAnnotationRule;
import com.impetus.kundera.validation.rules.IRule;
import com.impetus.kundera.validation.rules.RuleValidationException;


/**
 * @author Chhavi Gangwal
 *
 */
public class BootstrapValidationFactory extends AbstractValidationFactory implements ValidationFactory
{

    /**
     * List of rules for BootStrap level validations
     */
    static List<IRule> rules = new ArrayList<IRule>();
    
    static
    {
       rules.add(new EntityAnnotationRule());
       rules.add(new EntityFieldAnnotationRule());
    }
    
    /**
     * Constructor
     * adds static rule list to Rule factory
     */
    BootstrapValidationFactory()
    {
        this.ruleFactory.addRule(rules);
    }
    
    
    /* (non-Javadoc)
     * @see com.impetus.kundera.validation.AbstractValidationFactory#validate(java.lang.Class)
     */
    @Override
    public boolean validate(Class clazz) throws RuleValidationException 
    {
        return validate(clazz, this.ruleFactory.getJpaRules());
    }
        
}
