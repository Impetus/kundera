/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.persistence.handler.api;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;

/**
 * Interface to handle mapping.
 * 
 * @author vivek.mishra
 */
public interface MappingHandler
{

    /**
     * Handle association.
     * 
     * @param entity
     *            the entity
     * @param associationEntity
     *            the association entity
     * @param metadata
     *            the meta data.
     * @param relation
     *            holds entity relationships.
     * @return the entity save graph
     */
    EntitySaveGraph handleAssociation(Object entity, Object associationEntity, EntityMetadata metadata,
            Relation relation);
}
