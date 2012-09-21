/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Responsible for validating entity persistence
 * 
 * @author amresh.singh
 */
public class PersistenceValidator
{
    private static final Log log = LogFactory.getLog(PersistenceValidator.class);

    /**
     * Validates an entity object for CRUD operations
     * 
     * @param entity
     *            Instance of entity object
     * @return True if entity object is valid, false otherwise
     */
    public boolean isValidEntityObject(Object entity)
    {
        if (entity == null)
        {
            log.error("Entity to be persisted must not be null, operation failed");
            return false;
        }

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        if (metadata == null)
        {
            log.error(entity.getClass()
                    + " is not a JPA managed object as we couldn't find metadata for it. This object can't be persisted");
            return false;
        }

        Object id = PropertyAccessorHelper.getId(entity, metadata);
        if (id == null)
        {
            log.error("Entity to be persisted can't have Primary key set to null.");
            return false;
        }
        return true;
    }

}
