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
package com.impetus.kundera.persistence.handler.impl;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.api.MappingHandler;

/**
 * The Class ManyToManyHandler.
 * @author vivek.mishra
 */
public class ManyToManyHandler extends AssociationHandler implements MappingHandler
{

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.handler.api.MappingHandler#handleAssociation(java.lang.Object, java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.metadata.model.Relation)
     */
    @Override
    public EntitySaveGraph handleAssociation(Object entity, Object associationEntity, EntityMetadata metadata,Relation relation)
    {
        // Many to many is case of mapping table.
        //It is only possible via join table.
        //which means need to populate child entity explicitly.
        
        return populateDefaultGraph(associationEntity, entity, relation.getProperty());
    }

}
