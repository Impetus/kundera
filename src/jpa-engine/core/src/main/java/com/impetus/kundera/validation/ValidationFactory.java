/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at  new ValidationFactoryGenerator().getFactory(ValidationType type)
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
public interface ValidationFactory
{
    /**
     * validates whether a valid entity class or not 
     * 
     * @param clazz
     * @return
     * @throws RuleValidationException
     */
    boolean validate(Class clazz) throws RuleValidationException;
    
    /**
     * validates a given entity with given set of rules
     * 
     * @param clazz
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    boolean validate(Class clazz, IRule... rules) throws RuleValidationException;

    /**
     * validates a field of a class with given set of rules
     * 
     * @param field
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    boolean validate(Field field, IRule... rules) throws RuleValidationException;
    
    
    /**
     * validates a field against its value with given set of rules
     * 
     * @param field
     * @param fieldValue
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    boolean validate(Field field, Object fieldValue, IRule... rules) throws RuleValidationException;
       
    
    /**
     * RuleFactory to be used in different validation factories
     *
     */
    class RuleFactory
    {
        /**
         * list of jpa rules to be applied
         */
        private List<IRule> jpaRules = new ArrayList<IRule>();
        
        /**
         * @return
         */
        IRule[] getJpaRules()
        {
            IRule[] jrule = new IRule[jpaRules.size()];
            return (IRule[]) jpaRules.toArray(jrule);
        }

        /**
         * builds static rule list for validation factory
         * 
         * @param rules
         */
        void addRule(List<IRule> rules)
        {
            jpaRules.addAll(rules);
        }
    }
}
