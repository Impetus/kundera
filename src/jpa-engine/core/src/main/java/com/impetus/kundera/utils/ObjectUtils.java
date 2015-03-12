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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Type.PersistenceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.proxy.collection.ProxyCollection;

/**
 * Provides utility methods for operation on objects.
 * 
 * @author amresh.singh
 */
public class ObjectUtils
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(ObjectUtils.class);

    /**
     * Deep copy.
     * 
     * @param source
     *            the source
     * @param kunderaMetadata
     *            the kundera metadata
     * @return the object
     */
    public static final Object deepCopy(Object source, final KunderaMetadata kunderaMetadata)
    {
        Map<Object, Object> copiedObjectMap = new HashMap<Object, Object>();

        Object target = deepCopyUsingMetadata(source, copiedObjectMap, kunderaMetadata);

        copiedObjectMap.clear();
        copiedObjectMap = null;

        return target;
    }

    /**
     * Deep copy using metadata.
     * 
     * @param source
     *            the source
     * @param copiedObjectMap
     *            the copied object map
     * @param kunderaMetadata
     *            the kundera metadata
     * @return the object
     */
    private static Object deepCopyUsingMetadata(Object source, Map<Object, Object> copiedObjectMap,
            final KunderaMetadata kunderaMetadata)
    {
        Object target = null;
        try
        {
            if (source == null)
            {
                return null;
            }
            Class<?> sourceObjectClass = source.getClass();
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, sourceObjectClass);
            if (metadata == null)
            {
                return source;
            }

            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

            EntityType entityType = metaModel.entity(sourceObjectClass);

            // May break for mapped super class.

            Object id = null;
            if (metadata.getRelations() != null && !metadata.getRelations().isEmpty())
            {

                id = PropertyAccessorHelper.getId(source, metadata);

                StringBuilder keyBuilder = new StringBuilder(sourceObjectClass.getName());
                keyBuilder.append("#");
                keyBuilder.append(id);
                Object copiedObjectInMap = copiedObjectMap.get(keyBuilder.toString());
                if (copiedObjectInMap != null)
                {
                    return copiedObjectInMap;
                }
            }
            // Copy Columns (in a table that doesn't have any embedded objects

            target = KunderaCoreUtils.createNewInstance(sourceObjectClass);

            Iterator<Attribute> iter = entityType.getAttributes().iterator();
            while (iter.hasNext())
            {
                Attribute attrib = iter.next();

                Field columnField = (Field) attrib.getJavaMember();
                if (attrib.getPersistentAttributeType().equals(PersistentAttributeType.EMBEDDED)
                        || attrib.getPersistentAttributeType().equals(PersistentAttributeType.ELEMENT_COLLECTION))
                {
                    EmbeddableType embeddedColumn = metaModel.embeddable(((AbstractAttribute) attrib)
                            .getBindableJavaType());

                    Object sourceEmbeddedObj = PropertyAccessorHelper.getObject(source, columnField);
                    if (sourceEmbeddedObj != null)
                    {
                        if (metaModel.isEmbeddable(((AbstractAttribute) attrib).getBindableJavaType()))
                        {
                            if (attrib.isCollection())
                            {
                                Class<?> ecDeclaredClass = columnField.getType();
                                Class<?> actualEcObjectClass = sourceEmbeddedObj.getClass();

                                Object targetCollectionObject = actualEcObjectClass.newInstance();

                                // Copy element collections for List and Set
                                if (sourceEmbeddedObj instanceof Collection)
                                {
                                    Class<?> genericClass = PropertyAccessorHelper.getGenericClass(columnField);

                                    for (Object sourceEcObj : (Collection) sourceEmbeddedObj)
                                    {
                                        Object targetEcObj = null;
                                        if (sourceEcObj != null
                                                && PersistenceType.BASIC.equals(embeddedColumn.getPersistenceType()))
                                        {
                                            PropertyAccessor accessor = PropertyAccessorFactory
                                                    .getPropertyAccessor(sourceEcObj.getClass());
                                            if (accessor != null)
                                            {
                                                targetEcObj = accessor.getCopy(sourceEcObj);
                                            }
                                        }
                                        else if (sourceEcObj != null
                                                && PersistenceType.EMBEDDABLE.equals(embeddedColumn
                                                        .getPersistenceType()))
                                        {
                                            targetEcObj = genericClass.newInstance();

                                            for (Field f : genericClass.getDeclaredFields())
                                            {
                                                if (f != null && !Modifier.isStatic(f.getModifiers()))
                                                {
                                                    PropertyAccessorHelper.set(targetEcObj, f,
                                                            PropertyAccessorHelper.getObjectCopy(sourceEcObj, f));
                                                }
                                            }
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
                                }

                                // Copy element collection for Map
                                else if (sourceEmbeddedObj instanceof Map)
                                {
                                    for (Object sourceKey : ((Map) sourceEmbeddedObj).keySet())
                                    {
                                        Object targetKey = null;
                                        Object targetValue = null;
                                        if (PersistenceType.BASIC.equals(embeddedColumn.getPersistenceType())
                                                || PersistenceType.EMBEDDABLE.equals(embeddedColumn
                                                        .getPersistenceType()))
                                        {
                                            // Create copy of map key
                                            if (sourceKey != null)
                                            {
                                                PropertyAccessor keyAccessor = PropertyAccessorFactory
                                                        .getPropertyAccessor(sourceKey.getClass());
                                                if (keyAccessor != null)
                                                {
                                                    targetKey = keyAccessor.getCopy(sourceKey);
                                                }
                                            }
                                            else
                                            {
                                                targetKey = null;
                                            }

                                            // Create copy of map value
                                            Object sourceValue = ((Map) sourceEmbeddedObj).get(sourceKey);
                                            if (sourceValue != null)
                                            {
                                                PropertyAccessor valueAccessor = PropertyAccessorFactory
                                                        .getPropertyAccessor(sourceValue.getClass());
                                                if (valueAccessor != null)
                                                {
                                                    targetValue = valueAccessor.getCopy(sourceValue);
                                                }
                                            }
                                            else
                                            {
                                                targetValue = null;
                                            }

                                            if (Map.class.isAssignableFrom(ecDeclaredClass))
                                            {
                                                Method m = actualEcObjectClass.getMethod("put", new Class[] {
                                                        Object.class, Object.class });
                                                m.invoke(targetCollectionObject,
                                                        new Object[] { targetKey, targetValue });
                                            }
                                        }
                                    }
                                }
                                PropertyAccessorHelper.set(target, columnField, targetCollectionObject);
                            }
                            else
                            {
                                // Copy embedded objects
                                Class<?> embeddedColumnClass = columnField.getType();
                                Object targetEmbeddedObj = embeddedColumnClass.newInstance();

                                Set<Attribute> columns = embeddedColumn.getAttributes();
                                for (Attribute column : columns)
                                {
                                    PropertyAccessorHelper.set(
                                            targetEmbeddedObj,
                                            (Field) column.getJavaMember(),
                                            PropertyAccessorHelper.getObjectCopy(sourceEmbeddedObj,
                                                    (Field) column.getJavaMember()));
                                }

                                PropertyAccessorHelper.set(target, columnField, targetEmbeddedObj);
                            }
                        }
                        else if (((AbstractAttribute) attrib).getJPAColumnName() != null)
                        {
                            // Copy columns
                            PropertyAccessorHelper.set(target, columnField, sourceEmbeddedObj);
                        }
                    }

                }
                else if (attrib.getPersistentAttributeType().equals(PersistentAttributeType.BASIC))
                {

                    PropertyAccessorHelper.set(target, columnField,
                            PropertyAccessorHelper.getObjectCopy(source, columnField));
                }
            }

            // Put this object into copied object map
            if (id != null)
            {
                StringBuilder keyBuilder = new StringBuilder(sourceObjectClass.getName());
                keyBuilder.append("#");
                keyBuilder.append(id);
                copiedObjectMap.put(keyBuilder.toString(), target);
            }
            // Copy Relationships recursively
            for (Relation relation : metadata.getRelations())
            {
                if (relation != null)
                {
                    Field relationField = relation.getProperty();
                    Object sourceRelationObject = PropertyAccessorHelper.getObject(source, relationField);

                    if (sourceRelationObject != null)
                    {
                        if (sourceRelationObject instanceof KunderaProxy)
                        {
                            PropertyAccessorHelper.set(target, relationField, sourceRelationObject);
                            continue;
                        }
                        else if (ProxyHelper.isPersistentCollection(sourceRelationObject))
                        {
                            PropertyAccessorHelper.set(target, relationField, sourceRelationObject);
                            continue;
                        }
                        else if (ProxyHelper.isKunderaProxyCollection(sourceRelationObject))
                        {
                            ProxyCollection pc = ((ProxyCollection) sourceRelationObject).getCopy();
                            pc.setOwner(target);
                            PropertyAccessorHelper.set(target, relationField, pc);
                            continue;
                        }

                        Object targetRelationObject = null;

                        Class<?> relationObjectClass = relation.getProperty().getType();
                        Class<?> actualRelationObjectClass = sourceRelationObject.getClass();

                        if (Collection.class.isAssignableFrom(relationObjectClass))
                        {
                            targetRelationObject = actualRelationObjectClass.newInstance();
                            Method m = actualRelationObjectClass.getMethod("add", Object.class);

                            for (Object obj : (Collection) sourceRelationObject)
                            {
                                Object copyTargetRelObj = searchInCacheThenCopy(copiedObjectMap, obj, kunderaMetadata);
                                m.invoke(targetRelationObject, copyTargetRelObj);
                            }
                        }
                        else if (Map.class.isAssignableFrom(relationObjectClass))
                        {
                            targetRelationObject = actualRelationObjectClass.newInstance();
                            Method m = actualRelationObjectClass.getMethod("put", new Class<?>[] { Object.class,
                                    Object.class });

                            for (Object keyObj : ((Map) sourceRelationObject).keySet())
                            {
                                Object valObj = ((Map) sourceRelationObject).get(keyObj);

                                Object copyTargetKeyObj = searchInCacheThenCopy(copiedObjectMap, keyObj,
                                        kunderaMetadata);

                                Object copyTargetValueObj = searchInCacheThenCopy(copiedObjectMap, valObj,
                                        kunderaMetadata);

                                m.invoke(targetRelationObject, new Object[] { copyTargetKeyObj, copyTargetValueObj });
                            }
                        }
                        else
                        {
                            targetRelationObject = searchInCacheThenCopy(copiedObjectMap, sourceRelationObject,
                                    kunderaMetadata);
                        }
                        PropertyAccessorHelper.set(target, relationField, targetRelationObject);
                    }
                }
            }
        }
        catch (InstantiationException e)
        {
            log.warn("Error while instantiating entity/ embeddable class, did you define no-arg constructor?, Caused by:"
                    + e.getMessage());
            return null;
        }
        catch (Exception e)
        {
            log.warn("Returning null as error during clone, Caused by:" + e.getMessage());
            return null;
        }

        return target;
    }

    /**
     * Search in cache then copy.
     * 
     * @param copiedObjectMap
     *            the copied object map
     * @param sourceObject
     *            the source object
     * @param kunderaMetadata
     *            the kundera metadata
     * @return the object
     */
    private static Object searchInCacheThenCopy(Map<Object, Object> copiedObjectMap, Object sourceObject,
            final KunderaMetadata kunderaMetadata)
    {
        Object copyTargetRelObj = null;
        copyTargetRelObj = deepCopyUsingMetadata(sourceObject, copiedObjectMap, kunderaMetadata);
        return copyTargetRelObj;
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
