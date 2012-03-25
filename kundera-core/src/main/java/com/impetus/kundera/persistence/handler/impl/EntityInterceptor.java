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

import com.impetus.kundera.metadata.KunderaMetadataManager;
import java.util.ArrayList;
import java.util.List;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.api.MappingHandler;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class EntityInterceptor.
 * 
 * @author vivek.mishra
 */
public final class EntityInterceptor
{
    /**
     * ON handling relations.
     * 
     * @param entity
     *            source entity
     * @param metadata
     *            source entity meta data.
     * @return the entity save graph
     */
    public List<EntitySaveGraph> handleRelation(Object entity, EntityMetadata metadata)
    {
        List<EntitySaveGraph> objectGraphs = new ArrayList<EntitySaveGraph>();
        List<Relation> relations = metadata.getRelations();
        Object rlEntity = null;
        EntitySaveGraph objectGraph = new EntitySaveGraph();
        objectGraph.setParentEntity(entity);

        // TODO : Need to find a way for recursive calls and by-pass in case
        // parent and child belongs to same Store!

        if (relations.isEmpty())
        {
            objectGraphs.add(objectGraph);
            return objectGraphs;
        }

        for (Relation relation : relations)
        {
            Relation.ForeignKey relationType = relation.getType();

            try
            {
                rlEntity = PropertyAccessorHelper.getObject(entity, relation.getProperty());
            }
            catch (PropertyAccessException pex)
            {
                // TODO Prepare business exception class.
                throw new RuntimeException(pex.getMessage());
            }

            MappingHandler handler = getHandlerInstance(relationType);

            objectGraph = handler.handleAssociation(entity, rlEntity, metadata, relation);
            objectGraphs.add(objectGraph);

            /*Recursion
            EntityMetadata entityMetadata = KunderaMetadataManager
                    .getEntityMetadata(objectGraph.getChildClass(), metadata.getPersistenceUnit());
            
            List<EntitySaveGraph> recursiveRelations;
            try
            {
                recursiveRelations = handleRelation(
                    objectGraph.getChildClass().newInstance(), entityMetadata);
                
                objectGraphs.addAll(recursiveRelations);
            }
            catch (InstantiationException ex)
            {
                throw new RuntimeException(ex.getMessage());
            }
            catch (IllegalAccessException ex)
            {
                throw new RuntimeException(ex.getMessage());
            }
             */

            // object graph
            // If it is unidirectional, then detach child from parent.
            // if it is bidirectional, then detach both of them.(disjoint of
            // them)
            // once done with detacher, call to specific client for persistence.

            // No need of detacher.

            // Another case is : If returning child entity is also holding up
            // relations. then need to prepare a chain of calls. that will be
            // recursive call.
            // handler.handleAssociation(entity, metadata);

        }

        return objectGraphs;
        // At the end of for loop, there is a list of objects to be persisted
        // sequentially.
    }

    /**
     * Gets the handler instance.
     * 
     * @param key
     *            the key
     * @return the handler instance
     */
    private MappingHandler getHandlerInstance(Relation.ForeignKey key)
    {
        MappingHandler handler = null;
        switch (key)
        {
        case ONE_TO_ONE:
            handler = new OneToOneHandler();
            break;
        case ONE_TO_MANY:
            handler = new OneToManyHandler();
            break;
        case MANY_TO_ONE:
            handler = new ManyToOneHandler();
            break;
        case MANY_TO_MANY:
            handler = new ManyToManyHandler();
            break;

        default:
            break;
        }

        return handler;

    }
}