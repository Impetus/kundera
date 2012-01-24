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

import java.lang.reflect.Field;

import javax.persistence.OneToMany;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.api.MappingHandler;

/**
 * The Class ManyToOneHandler.
 * 
 * @author vivek.mishra
 */
public class ManyToOneHandler extends AssociationHandler implements MappingHandler
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.handler.api.MappingHandler#handleAssociation
     * (java.lang.Object, java.lang.Object,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.metadata.model.Relation)
     */
    @Override
    public EntitySaveGraph handleAssociation(Object entity, Object associationEntity, EntityMetadata metadata,
            Relation relation)
    {
        // In all cases, parent entity will become child.
        EntitySaveGraph objectGraph = populateDefaultGraph(entity, associationEntity, relation.getProperty());
        Field field = computeDirection(entity, relation.getProperty(), objectGraph, OneToMany.class);
        onDetach(entity, associationEntity, relation.getProperty(), true);
        if (!objectGraph.isUniDirectional())
        {
            onDetach(associationEntity, entity, objectGraph.getBidirectionalProperty(), true);
        }
        objectGraph.setIsswapped(true);
        return objectGraph;
    }

}
