/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.rdbms;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.attributes.AttributeType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;

/**
 * The Class HibernateUtils.
 * 
 * @author vivek.mishra
 */
public final class HibernateUtils
{

    /**
     * Gets the properties.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the properties
     */
    static final Properties getProperties(final KunderaMetadata kunderaMetadata, final String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadatata.getProperties();
        return props;
    }

    static final URL getPersistenceUnitUrl(final KunderaMetadata kunderaMetadata, final String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        return persistenceUnitMetadatata != null ? persistenceUnitMetadatata.getMappedUrl() : null;
    }

    /**
     * 
     * @param entity
     * @param valueMap
     * @param m
     * @return
     */
    static Map<String, Object> getTranslatedObject(final KunderaMetadata kunderaMetadata, Object entity, Map<String, Object> valueMap, EntityMetadata m)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        Object rowKey = valueMap.get(idColumnName) == null ? valueMap.get(idColumnName.toUpperCase()) == null ? valueMap
                .get(idColumnName.toLowerCase()) : valueMap.get(idColumnName.toUpperCase())
                : valueMap.get(idColumnName);

        Map<String, Object> relationValue = new HashMap<String, Object>();

        if (valueMap != null && (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()) || rowKey != null))
        {
            try
            {
                if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
                {
                    EmbeddableType embeddable = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
                    Iterator<Attribute> iter = embeddable.getAttributes().iterator();
                    Object compoundKey = m.getIdAttribute().getBindableJavaType().newInstance();
                    while (iter.hasNext())
                    {
                        Attribute attr = iter.next();
                        AbstractAttribute compositeAbstractAttrib = (AbstractAttribute) attr;
                        Object value = valueMap.get(compositeAbstractAttrib.getJPAColumnName()) == null ? valueMap
                                .get(compositeAbstractAttrib.getJPAColumnName().toUpperCase()) == null ? valueMap
                                .get(compositeAbstractAttrib.getJPAColumnName().toLowerCase()) : valueMap
                                .get(compositeAbstractAttrib.getJPAColumnName().toUpperCase()) : valueMap
                                .get(compositeAbstractAttrib.getJPAColumnName());
                        setFieldValue(compoundKey, value, attr);
                    }
                    PropertyAccessorHelper.setId(entity, m, compoundKey);
                }
                else
                {
                    setId(entity, rowKey, m);
                }

                Set<Attribute> columns = entityType.getAttributes();

                for (Attribute column : columns)
                {
                    if (!column.equals(m.getIdAttribute()))
                    {
                        String jpaColumnName = ((AbstractAttribute) column).getJPAColumnName();

                        Class javaType = ((AbstractAttribute) column).getBindableJavaType();
                        if (metaModel.isEmbeddable(javaType))
                        {
                            onViaEmbeddable(column, entity, metaModel, valueMap);
                        }
                        else if (!column.isAssociation())
                        {
                            Object valueObject = valueMap.get(jpaColumnName) == null ? valueMap.get(jpaColumnName
                                    .toUpperCase()) == null ? valueMap.get(jpaColumnName.toLowerCase()) : valueMap
                                    .get(jpaColumnName.toUpperCase()) : valueMap.get(jpaColumnName);
                            if (valueObject != null && AttributeType.getType(javaType).equals(AttributeType.ENUM))
                            {
                                EnumAccessor accessor = new EnumAccessor();
                                valueObject = accessor.fromString(javaType, valueObject.toString());
                                setFieldValue(entity, valueObject, column);
                            }
                            else if(valueObject != null)
                            {
                                setFieldValue(entity, valueObject, column);
                            }
                        }
                        else if (m.getRelationNames() != null)
                        {
                            if (m.getRelationNames().contains(jpaColumnName) && !jpaColumnName.equals(idColumnName))
                            {
                                Object colValue = valueMap.get(jpaColumnName) == null ? valueMap.get(jpaColumnName
                                        .toUpperCase()) == null ? valueMap.get(jpaColumnName.toLowerCase()) : valueMap
                                                .get(jpaColumnName.toUpperCase()) : valueMap.get(jpaColumnName);
                                if (colValue != null)
                                {
                                    relationValue.put(jpaColumnName, colValue);
                                }
                            }
                        }
                    }
                }
                return relationValue;
            }
            catch (Exception ex)
            {
                throw new EntityReaderException(ex);
            }
        }
        throw new EntityReaderException("Can not be translated into entity.");
    }

    /**
     * @param entityType
     * @param column
     * @param m
     * @param entity
     * @param embeddable
     * @param valueMap
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private static void onViaEmbeddable(Attribute column, Object entity, Metamodel metamodel,
            Map<String, Object> valueMap) throws InstantiationException, IllegalAccessException
    {
        EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) column).getBindableJavaType());
        Field embeddedField = (Field) column.getJavaMember();
        Object embeddedDocumentObject = null;

        if (column.isCollection())
        {
            Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(embeddedField);

            embeddedDocumentObject = valueMap.get(((AbstractAttribute) column).getJPAColumnName());

            if (embeddedDocumentObject != null)
            {
                Collection embeddedCollection = getCollectionFromDocumentList(metamodel,
                        (List<Map<String, Object>>) embeddedDocumentObject, embeddedField.getType(),
                        embeddedObjectClass, embeddable.getAttributes());
                PropertyAccessorHelper.set(entity, embeddedField, embeddedCollection);
            }
        }
        else
        {
            Object obj = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
            if (obj == null)
            {
                obj = ((AbstractAttribute) column).getBindableJavaType().newInstance();
            }
            embeddedDocumentObject = valueMap.get(((AbstractAttribute) column).getJPAColumnName());
            if(embeddedDocumentObject != null)
            PropertyAccessorHelper.set(
                    entity,
                    embeddedField,
                    getObjectFromDocument(metamodel, (Map<String, Object>) embeddedDocumentObject,
                            embeddable.getAttributes(), obj));
        }
    }

    /**
     * Creates a collection of <code>embeddedObjectClass</code> instances
     * wherein each element is java object representation of MongoDB document
     * object contained in <code>documentList</code>. Field names are determined
     * from <code>columns</code>.
     * 
     * @param documentList
     *            the document list
     * @param embeddedCollectionClass
     *            the embedded collection class
     * @param embeddedObjectClass
     *            the embedded object class
     * @param columns
     *            the columns
     * @param metamodel
     * @return the collection from document list
     */
    private static Collection<?> getCollectionFromDocumentList(Metamodel metamodel,
            List<Map<String, Object>> documentList, Class embeddedCollectionClass, Class embeddedObjectClass,
            Set<Attribute> columns)
    {
        Collection<Object> embeddedCollection = null;
        if (embeddedCollectionClass.equals(Set.class))
        {
            embeddedCollection = new HashSet<Object>();
        }
        else if (embeddedCollectionClass.equals(List.class))
        {
            embeddedCollection = new ArrayList<Object>();
        }
        else
        {
            throw new PersistenceException("Invalid collection class " + embeddedCollectionClass
                    + "; only Set and List allowed");
        }

        for (Map<String, Object> dbObj : documentList)
        {
            try
            {
                Object obj = embeddedObjectClass.newInstance();
                embeddedCollection.add(getObjectFromDocument(metamodel, dbObj, columns, obj));
            }
            catch (InstantiationException e)
            {
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                throw new PersistenceException(e);
            }
        }
        return embeddedCollection;
    }

    /**
     * Creates an instance of <code>clazz</code> and populates fields fetched
     * from MongoDB document object. Field names are determined from
     * <code>columns</code>
     * 
     * @param documentObj
     *            the document obj
     * @param clazz
     *            the clazz
     * @param columns
     *            the columns
     * @return the object from document
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private static Object getObjectFromDocument(Metamodel metamodel, Map<String, Object> documentObj,
            Set<Attribute> columns, Object obj) throws InstantiationException, IllegalAccessException
    {
        for (Attribute column : columns)
        {
            Object value = documentObj.get(((AbstractAttribute) column).getJPAColumnName());

            if (((MetamodelImpl) metamodel).isEmbeddable(((AbstractAttribute) column).getBindableJavaType()))
            {
                onViaEmbeddable(column, obj, metamodel, (Map<String, Object>) value);
            }
            else
            {
                setFieldValue(obj, value, column);
            }
        }
        return obj;
    }

    private static void setFieldValue(Object entity, Object value, Attribute column)
    {
        value = PropertyAccessorHelper.fromSourceToTargetClass(column.getJavaType(), value.getClass(), value);
        PropertyAccessorHelper.set(entity, (Field) column.getJavaMember(), value);
    }

    private static void setId(Object entity, Object value, EntityMetadata m)
    {
        value = PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute().getJavaType(), value.getClass(),
                value);
        PropertyAccessorHelper.setId(entity, m, value);
    }
}
