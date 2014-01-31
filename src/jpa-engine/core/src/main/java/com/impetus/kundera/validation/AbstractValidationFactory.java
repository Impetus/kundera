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

import com.impetus.kundera.validation.rules.EntityRule;
import com.impetus.kundera.validation.rules.FieldRule;
import com.impetus.kundera.validation.rules.IRule;
import com.impetus.kundera.validation.rules.RuleValidationException;

/**
 * @author Chhavi Gangwal
 *
 */
public abstract class AbstractValidationFactory
{
            
    /**
     * rule factory object 
     */
    protected com.impetus.kundera.validation.ValidationFactory.RuleFactory ruleFactory = new com.impetus.kundera.validation.ValidationFactory.RuleFactory();
    
    
    /**
     * @param clazz
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    public boolean validate(Class clazz, IRule... rules) throws RuleValidationException
    {
        
        for (IRule rule : rules)
        {
            ((EntityRule)rule).validate(clazz);
        }
        return true;
       
        
    }

    
    /**
     * @param field
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    public boolean validate(Field field, IRule... rules) throws RuleValidationException
    {
       
        for (IRule rule : rules)
        {
            ((FieldRule)rule).validate(field);
        }
        return true;
    }
    
   
    /**
     * @param field
     * @param fieldValue
     * @param rules
     * @return
     * @throws RuleValidationException
     */
    public boolean validate(Field field, Object fieldValue, IRule... rules) throws RuleValidationException
    {
       
        for (IRule rule : rules)
        {
            ((FieldRule)rule).validate(field, fieldValue);
        }
        return true;
    }
    

    /**
     * @param clazz
     * @return
     * @throws RuleValidationException
     */
    public boolean validate(Class clazz) throws RuleValidationException 
    {
        throw new UnsupportedOperationException("Bootstrap level validations are not supported at operation level!");
    }
    
}
