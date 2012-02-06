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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import javax.persistence.JoinColumn;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class AssociationHandler.
 * 
 * @author vivek.mishra
 */
class AssociationHandler
{

    /**
     * Instantiates a new association handler.
     */
    public AssociationHandler()
    {
        super();
    }

    /**
     * Gets the relation field name.
     * 
     * @param relation
     *            the relation
     * @return the relation field name
     */
    protected String getJoinColumnName(Field relation)
    {
        String columnName = null;
        JoinColumn ann = relation.getAnnotation(JoinColumn.class);
        if (ann != null)
        {
            columnName = ann.name();

        }
        return columnName != null ? columnName : relation.getName();
    }

    /**
     * Populate default graph.
     * 
     * @param entity
     *            the entity
     * @param associatedEntity
     *            the associated entity
     * @param rField
     *            the r field
     * @return the entity save graph
     */

    protected EntitySaveGraph populateDefaultGraph(Object entity, Object associatedEntity, Field rField)
    {
        EntitySaveGraph objectGraph = new EntitySaveGraph(rField);
        objectGraph.setParentEntity(associatedEntity);
        objectGraph.setChildEntity(entity);
        objectGraph.setfKeyName(getJoinColumnName(rField));
        return objectGraph;
    }

    /**
     * Compute direction.
     * 
     * @param entity
     *            the entity
     * @param associatedEntity
     *            the associated entity
     * @param objectGraph
     *            the object graph
     * @return the field
     */

    // TODO: this can be moved to metadata level.
    protected <T extends Annotation> Field computeDirection(Object entity, Field relationalField,
            EntitySaveGraph objectGraph, Class<T> clazz)
    {
        Field[] fields = PropertyAccessorHelper.getDeclaredFields(relationalField);

        Class<?> clazzz = null;
        for (Field field : fields)
        {
            if (field.isAnnotationPresent(clazz))
            {
                clazzz = field.getType();
                if (PropertyAccessorHelper.isCollection(clazzz))
                {
                    ParameterizedType type = (ParameterizedType) field.getGenericType();
                    Type[] types = type.getActualTypeArguments();
                    clazzz = (Class<?>) types[0];
                }

                if (clazzz.equals(entity.getClass()))
                {
                    // then it is a case of bi directional.
                    objectGraph.setUniDirectional(false);
                    objectGraph.setBidirectionalProperty(field);
                    return field;
                }
            }

        }

        return null;
    }

    /**
     * Removed association entit(ies) from the enclosing entity
     * 
     * @param entity
     * @param associationEntity
     * @param field
     * @param setNull
     */
    protected void onDetach(Object entity, Object associationEntity, Field field, boolean setNull)
    {

        try
        {
            if (entity != null)
            {
                if (entity instanceof Collection<?>)
                {
                    Collection<?> entityCollection = (Collection<?>) entity;
                    for (Object entityObj : entityCollection)
                    {
                        if (entityObj != null)
                        {
                            PropertyAccessorHelper.set(entityObj, field, setNull || entity == null ? null
                                    : Collection.class.isAssignableFrom(field.getType()) ? null : associationEntity
                                            .getClass().newInstance());
                        }

                    }

                }
                else
                {
                    PropertyAccessorHelper.set(entity, field, setNull || associationEntity == null ? null
                            : associationEntity.getClass().newInstance());
                }
            }
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }
    }

}