/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.impetus.kundera.validation.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Check if primary key is null 
 * 
 * @author vivek.mishra
 * 
 * @param <E> @Id attribute.
 */
public class PrimaryKeyNullCheck<E extends Object>
{
    private static final Logger log = LoggerFactory.getLogger(PrimaryKeyNullCheck.class);


    /* (non-Javadoc)
     * @see com.impetus.kundera.validation.rules.IRule#validate(java.lang.Object)
     */
    
    public boolean validate(E value)
    {
        if (value == null)
        {
            log.error("Entity to be persisted can't have Primary key set to null.");
            throw new IllegalArgumentException("Entity to be persisted can't have Primary key set to null.");
        }
        
        return true;
    }

}
