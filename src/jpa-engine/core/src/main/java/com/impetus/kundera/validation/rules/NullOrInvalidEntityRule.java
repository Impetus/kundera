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

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Check if entity is not null and contains valid entity metadata
 * 
 * @author vivek.mishra
 * 
 * @param <E>
 */
public class NullOrInvalidEntityRule<E extends Object>
{

    private EntityMetadata entityMetadata;

    public NullOrInvalidEntityRule(EntityMetadata entityMetadata)
    {
        this.entityMetadata = entityMetadata;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.validation.rules.IRule#validate(java.lang.Object)
     */
    
    public boolean validate(E entity)
    {
        if (entity != null)
        {
            // entity metadata could be null.
            if (entityMetadata == null)
            {
                throw new IllegalArgumentException(
                        "Entity object is invalid, operation failed. Please check previous log message for details");
            }

            return false;

        }

        // will return false if entity is null.
        return true;
    }

}
