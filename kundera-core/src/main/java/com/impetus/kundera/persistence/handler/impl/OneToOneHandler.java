/*******************************************************************************
s * * Copyright 2011 Impetus Infotech.
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

import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;
import javax.persistence.PrimaryKeyJoinColumn;

import org.hibernate.proxy.HibernateProxy;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.handler.api.MappingHandler;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

//TODO need to think of multiple relationships. 
//A->B = output says( B->A) and other relation says (A->C). so overall output is : C->B->A (need to look into this later).

//TODO: Look into what is PrimaryKeyJoinColumns annotation

/**
 * The Class OneToOneHandler.
 * 
 * @author vivek.mishra
 */
class OneToOneHandler extends AssociationHandler implements MappingHandler
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
    public EntitySaveGraph handleAssociation(Object entity, Object associatedEntity, EntityMetadata metadata,
            Relation relation)
    {

        Field rField = relation.getProperty();
        EntitySaveGraph objectGraph = populateDefaultGraph(entity, associatedEntity, rField);

        isSharedByPrimaryKey(entity, rField, objectGraph, associatedEntity, metadata);
        computeDirection(entity, relation.getProperty(), objectGraph, OneToOne.class);
        onDetach(entity, associatedEntity, relation.getProperty(), true);
        // Read annotation over field, if it is joinByPrimaryKey then consider
        // populating primary key of associated table as of entity.
        // let say if u able to read. Person(String personId, name) and
        // Address(String addressId).
        // read entity, get list of relational entities
        // 1) if Relation entity require id from source entity get it. (first
        // persist source entity
        // 2)

        objectGraph.setProperty(rField);

        return objectGraph;
    }

    /**
     * Checks if is shared by primary key.
     *
     * @param entity the entity
     * @param rField the r field
     * @param objectGraph the object graph
     * @param associatedEntity the associated entity
     * @param metadata the metadata
     */
    private void isSharedByPrimaryKey(Object entity, Field rField, EntitySaveGraph objectGraph,
            Object associatedEntity, EntityMetadata metadata)
    {
        if (rField.isAnnotationPresent(PrimaryKeyJoinColumn.class))
        {
            // Means it is a case of populating associatedEntity's primary key
            // with entity's primary key.

            populatePKey(entity, associatedEntity, metadata);
            objectGraph.setParentEntity(entity);
            objectGraph.setChildEntity(associatedEntity);
            objectGraph.setSharedPrimaryKey(true);
            objectGraph.setfKeyName(metadata.getIdColumn().getName());
        }
        else
        {
            objectGraph.setIsswapped(true);
        }
    }

    // TODO: if getting metadata via class is possible.this method will not be
    // required. refactor this one finish that.

    /**
     * Populate p key.
     *
     * @param entity the entity
     * @param associatedEntity the associated entity
     * @param metadata the metadata
     */
    private void populatePKey(Object entity, Object associatedEntity, EntityMetadata metadata)
    {
        if (associatedEntity != null)
        {
            Class<?> clazz = associatedEntity.getClass();
            if (associatedEntity instanceof HibernateProxy)
            {
                clazz = associatedEntity.getClass().getSuperclass();
            }
            Field[] fields = clazz.getDeclaredFields();
            Field f = null;
            for (Field field : fields)
            {
                if (field.isAnnotationPresent(Id.class))
                {
                    f = field;
                    break;
                }

            }

            try
            {
                PropertyAccessorHelper.setId(associatedEntity, metadata, getId(entity, metadata));
                // PropertyAccessorHelper.set(associatedEntity, f, getId(entity,
                // metadata));
            }
            catch (PropertyAccessException e)
            {
                throw new PersistenceException(e.getMessage());
            }
        }
    }

    /**
     * Gets the id.
     *
     * @param entity the entity
     * @param metadata the metadata
     * @return the id
     */
    private String getId(Object entity, EntityMetadata metadata)
    {
        try
        {
            return PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e.getMessage());
        }

    }

}