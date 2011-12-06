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
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.persistence.handler.impl;

import java.lang.reflect.Field;

import javax.persistence.ManyToOne;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.api.MappingHandler;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class OneToManyHandler.
 */
class OneToManyHandler extends AssociationHandler implements MappingHandler
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.persistence.handler.api.MappingHandler#handleAssociation
     * (java.lang.Object, java.lang.Object,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public EntitySaveGraph handleAssociation(Object entity, Object associationEntity, EntityMetadata metadata,
            Relation relation)
    {
        // IF Mapping is unidirectional then associationEntity will be child
        // else parent will be child.

        EntitySaveGraph objectGraph = getDirectionalGraph(entity, associationEntity, relation);

        return objectGraph;
    }

    /**
     * Gets the directional graph.
     * 
     * @param entity
     *            the entity
     * @param fields
     *            the fields
     * @param associationEntity
     *            the association entity
     * @param relation
     *            the relation
     * @return the directional graph
     */
    private EntitySaveGraph getDirectionalGraph(Object entity, Object associationEntity, Relation relation)
    {
        EntitySaveGraph objectGraph = new EntitySaveGraph(relation.getProperty());
        objectGraph.setChildEntity(associationEntity);
        objectGraph.setParentEntity(entity);
        Field field = computeDirection(entity, relation.getProperty(), objectGraph, ManyToOne.class);
//        objectGraph.setProperty(rField);

//        objectGraph.setParentClass(PropertyAccessorHelper.getGenericClass(relation.getProperty()));
//        objectGraph.setChildClass(entity.getClass());

        if (!objectGraph.isUniDirectional())
        {
            objectGraph.setfKeyName(getJoinColumnName(field));
            onDetach(entity, associationEntity, relation.getProperty(), false);
            // onDetach(associationEntity, entity, field, false);
            return objectGraph;
        }

        onDetach(entity, associationEntity, relation.getProperty(), false);
        // in case of uni-directional.
        objectGraph.setfKeyName(getJoinColumnName(relation.getProperty()));
        return objectGraph;
    }

}
