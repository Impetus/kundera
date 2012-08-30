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
package com.impetus.kundera.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.cloner.Cloner;
import org.hibernate.collection.AbstractPersistentCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Provides utility methods for operation on objects
 * 
 * @author amresh.singh
 */
public class ObjectUtils
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(ObjectUtils.class);

    public static final Object deepCopyUsingCloner(Object objectToCopy)
    {
        Object destObject;
        try
        {
            destObject = Cloner.deepClone(objectToCopy, objectToCopy.getClass());
        }
        catch (Exception e)
        {
            return null;
        }
        return destObject;

    }

    public static final Object deepCopy(Object source)
    {
        Map<Object, Object> copiedObjectMap = new HashMap<Object, Object>();

        Object target = deepCopyUsingMetadata(source, copiedObjectMap);

        copiedObjectMap.clear();
        copiedObjectMap = null;

        return target;
    }

    /**
     * @param source
     * @return
     */
    private static Object deepCopyUsingMetadata(Object source, Map<Object, Object> copiedObjectMap)
    {
        Object target = null;
        try
        {
            if (source == null)
                return null;

            Class<?> sourceObjectClass = source.getClass();
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(sourceObjectClass);
            if (metadata == null)
            {
                return null;
            }
            EntityType entityType = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit()).entity(sourceObjectClass);

            // May break for mapped super class. 
            
            Object id = PropertyAccessorHelper.getId(source, metadata);

            Object copiedObjectInMap = copiedObjectMap.get(sourceObjectClass.getName() + "#" + id);
            if (copiedObjectInMap != null)
            {
                return copiedObjectInMap;
            }

            // Copy Columns (in a table that doesn't have any embedded objects
            
            target = sourceObjectClass.newInstance();

            Iterator<Attribute> iter = entityType.getAttributes().iterator();
            while(iter.hasNext())
            {
                Attribute attrib = iter.next();
                
                Field columnField = (Field) attrib.getJavaMember();
                PropertyAccessorHelper.set(target, columnField, PropertyAccessorHelper.getObject(source, columnField));
            }
            
            // Copy Embedded Columns, Element Collections are columns (in a
            // table that only has embedded objects)
           
            //TODO: Above written code should be able to handle embedded attributes.
            
            /*for (EmbeddedColumn embeddedColumn : metadata.getEmbeddedColumnsAsList())
            {
                Field embeddedColumnField = embeddedColumn.getField();

                Object sourceEmbeddedObj = PropertyAccessorHelper.getObject(source, embeddedColumnField);
                if (sourceEmbeddedObj != null)
                {
                    if (embeddedColumnField.getAnnotation(Embedded.class) != null)
                    {
                        // Copy embedded objects
                        Class<?> embeddedColumnClass = embeddedColumnField.getType();
                        Object targetEmbeddedObj = embeddedColumnClass.newInstance();

                        for (Column column : embeddedColumn.getColumns())
                        {
                            PropertyAccessorHelper.set(targetEmbeddedObj, column.getField(),
                                    PropertyAccessorHelper.getObject(sourceEmbeddedObj, column.getField()));
                        }

                        PropertyAccessorHelper.set(target, embeddedColumnField, targetEmbeddedObj);
                    }
                    else if (embeddedColumnField.getAnnotation(ElementCollection.class) != null)
                    {
                        // Copy element collections
                        if (sourceEmbeddedObj instanceof Collection)
                        {
                            Class<?> ecDeclaredClass = embeddedColumnField.getType();
                            Class<?> actualEcObjectClass = sourceEmbeddedObj.getClass();
                            Class<?> genericClass = PropertyAccessorHelper.getGenericClass(embeddedColumnField);

                            Object targetCollectionObject = actualEcObjectClass.newInstance();

                            for (Object sourceEcObj : (Collection) sourceEmbeddedObj)
                            {
                                Object targetEcObj = genericClass.newInstance();
                                for (Field f : genericClass.getDeclaredFields())
                                {
                                    PropertyAccessorHelper.set(targetEcObj, f,
                                            PropertyAccessorHelper.getObject(sourceEcObj, f));

                                }

                                if (List.class.isAssignableFrom(ecDeclaredClass))
                                {
                                    Method m = actualEcObjectClass.getMethod("add", Object.class);
                                    m.invoke(targetCollectionObject, targetEcObj);

                                }
                                else if (Set.class.isAssignableFrom(ecDeclaredClass))
                                {
                                    Method m = actualEcObjectClass.getMethod("add", Object.class);
                                    m.invoke(targetCollectionObject, targetEcObj);
                                }

                            }
                            PropertyAccessorHelper.set(target, embeddedColumnField, targetCollectionObject);
                        }

                    }
                    else if (embeddedColumnField.getAnnotation(javax.persistence.Column.class) != null)
                    {
                        // Copy columns
                        PropertyAccessorHelper.set(target, embeddedColumnField, sourceEmbeddedObj);
                    }
                }
            }
*/
            // Put this object into copied object map
            copiedObjectMap.put(sourceObjectClass.getName() + "#" + id, target);

            // Copy Relationships recursively
            for (Relation relation : metadata.getRelations())
            {
                Field relationField = relation.getProperty();
                Object sourceRelationObject = PropertyAccessorHelper.getObject(source, relationField);

                if (sourceRelationObject != null && !(sourceRelationObject instanceof AbstractPersistentCollection))
                {
                    Object targetRelationObject = null;

                    Class<?> relationObjectClass = relation.getProperty().getType();
                    Class<?> actualRelationObjectClass = sourceRelationObject.getClass();

                    if (!Collection.class.isAssignableFrom(relationObjectClass))
                    {
                        targetRelationObject = deepCopyUsingMetadata(sourceRelationObject, copiedObjectMap);
                    }
                    else
                    {
                        targetRelationObject = actualRelationObjectClass.newInstance();
                        Method m = actualRelationObjectClass.getMethod("add", Object.class);

                        for (Object obj : (Collection) sourceRelationObject)
                        {
                            Object copyTargetRelObj = deepCopyUsingMetadata(obj, copiedObjectMap);

                            m.invoke(targetRelationObject, copyTargetRelObj);
                        }
                    }
                    // Put this object into copied object map
                    // copiedObjectMap.put(sourceObjectClass.getName() + "#" +
                    // id, target);
                    PropertyAccessorHelper.set(target, relationField, targetRelationObject);
                }

            }

        }
        catch (InstantiationException e)
        {
            log.warn("Returning null as error during clone, Caused by:" + e.getMessage());
            return null;
        }
        catch (IllegalAccessException e)
        {
            log.warn("Returning null as error during clone, Caused by:" + e.getMessage());
            return null;
        }

        catch (InvocationTargetException e)
        {
            log.warn("Returning null as error during clone, Caused by:" + e.getMessage());
            return null;
        }

        catch (NoSuchMethodException e)
        {
            log.warn("Returning null as error during clone, Caused by:" + e.getMessage());
            return null;
        }

        return target;
    }

    /**
     * Gets the field instance.
     * 
     * @param chids
     *            the chids
     * @param f
     *            the f
     * @return the field instance
     */
    public static Object getFieldInstance(List chids, Field f)
    {

        if (Set.class.isAssignableFrom(f.getType()))
        {
            Set col = new HashSet(chids);
            return col;
        }
        return chids;
    }

}
