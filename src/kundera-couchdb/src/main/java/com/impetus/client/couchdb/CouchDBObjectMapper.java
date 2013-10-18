/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.OperationNotSupportedException;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Object mapper for json
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBObjectMapper
{
    private static final Logger log = LoggerFactory.getLogger(CouchDBObjectMapper.class);

    /**
     * 
     * @param m
     * @param entity
     * @param id
     * @param relations
     * @return
     * @throws OperationNotSupportedException
     */
    static JsonObject getJsonOfEntity(EntityMetadata m, Object entity, Object id, List<RelationHolder> relations)
            throws OperationNotSupportedException
    {
        JsonObject jsonObject = new JsonObject();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        // Add discriminator column and value.
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discrValue != null)
        {
            jsonObject.add(discrColumn, getJsonPrimitive(discrValue, discrValue.getClass()));
        }
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            log.error("Composite key not supported in CouchDB as of now.");
            throw new OperationNotSupportedException("Composite key not supported in CouchDB as of now.");
        }
        else
        {
            jsonObject.addProperty("_id", m.getTableName() + PropertyAccessorHelper.getString(id));
            jsonObject.add(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName(),
                    getJsonPrimitive(id, m.getIdAttribute().getJavaType()));
        }
        // Populate columns
        Set<Attribute> columns = entityType.getAttributes();
        for (Attribute column : columns)
        {
            if (!column.equals(m.getIdAttribute()))
            {
                try
                {
                    Class javaType = ((AbstractAttribute) column).getBindableJavaType();
                    if (metaModel.isEmbeddable(javaType))
                    {
                        onEmbeddable(entityType, column, entity, metaModel.embeddable(javaType), jsonObject);
                    }
                    else if (!column.isAssociation())
                    {
                        Object valueObject = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
                        jsonObject.add(((AbstractAttribute) column).getJPAColumnName(),
                                getJsonPrimitive(valueObject, column.getJavaType()));
                    }
                }
                catch (PropertyAccessException paex)
                {
                    log.error("Can't access property {}.", column.getName());
                    throw new PropertyAccessException(paex);
                }
            }
        }
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                jsonObject.add(rh.getRelationName(),
                        getJsonPrimitive(rh.getRelationValue(), rh.getRelationValue().getClass()));
            }
        }
        return jsonObject;
    }

    /**
     * 
     * @param entityType
     * @param column
     * @param m
     * @param entity
     */
    private static void onEmbeddable(EntityType entityType, Attribute column, Object entity,
            EmbeddableType embeddableType, JsonObject jsonObject)
    {
        Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
        String embeddedColumnName = ((AbstractAttribute) column).getJPAColumnName();
        Set<Attribute> embeddableAttributes = embeddableType.getAttributes();
        jsonObject.add(embeddedColumnName, getJsonObject(embeddableAttributes, embeddedObject));
    }

    /**
     * 
     * @param columns
     * @param object
     * @return
     */
    private static JsonObject getJsonObject(Set<Attribute> columns, Object object)
    {
        JsonObject jsonObject = new JsonObject();
        for (Attribute column : columns)
        {
            if (!column.isAssociation())
            {
                Object valueObject = PropertyAccessorHelper.getObject(object, (Field) column.getJavaMember());
                jsonObject.add(((AbstractAttribute) column).getJPAColumnName(),
                        getJsonPrimitive(valueObject, column.getJavaType()));
            }
        }
        return jsonObject;
    }

    /**
     * 
     * @param entityClass
     * @param m
     * @param jsonObj
     * @param relations
     * @return
     */
    static Object getEntityFromJson(Class<?> entityClass, EntityMetadata m, JsonObject jsonObj, List<String> relations)
    {// Entity object
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        try
        {
            entity = entityClass.newInstance();

            // Populate primary key column
            JsonElement rowKey = jsonObj.get(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName());
            Class<?> rowKeyValueClass = rowKey.getClass();
            Class<?> idClass = null;
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            Map<String, Object> relationValue = null;
            idClass = m.getIdAttribute().getJavaType();
            if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
            {
                log.error("Composite key not supported in CouchDB as of now.");
                throw new OperationNotSupportedException("Composite key not supported in CouchDB as of now.");
            }
            else
            {
                PropertyAccessorHelper.setId(entity, m,
                        PropertyAccessorHelper.fromSourceToTargetClass(idClass, String.class, rowKey.getAsString()));
            }

            // Populate entity columns
            // List<Column> columns = m.getColumnsAsList();
            EntityType entityType = metaModel.entity(entityClass);

            String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();

            Set<Attribute> columns = entityType.getAttributes();

            for (Attribute column : columns)
            {
                JsonElement value = jsonObj.get(((AbstractAttribute) column).getJPAColumnName());
                if (!column.equals(m.getIdAttribute())
                        && !((AbstractAttribute) column).getJPAColumnName().equals(discriminatorColumn)
                        && value != null && !value.equals(JsonNull.INSTANCE))
                {
                    String fieldName = ((AbstractAttribute) column).getJPAColumnName();

                    Class javaType = ((AbstractAttribute) column).getBindableJavaType();
                    if (metaModel.isEmbeddable(javaType))
                    {
                        onViaEmbeddable(entityType, column, m, entity, metaModel.embeddable(javaType), jsonObj);
                    }
                    else if (!column.isAssociation())
                    {
                        setFieldValue(entity, column, value);
                    }
                    else if (relations != null)
                    {
                        if (relationValue == null)
                        {
                            relationValue = new HashMap<String, Object>();
                        }

                        if (relations.contains(fieldName)
                                && !fieldName.equals(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()))
                        {
                            JsonElement colValue = jsonObj.get(((AbstractAttribute) column).getJPAColumnName());
                            if (colValue != null)
                            {
                                String colFieldName = m.getFieldName(fieldName);
                                Attribute attribute = entityType.getAttribute(colFieldName);
                                EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(attribute
                                        .getJavaType());
                                Object colVal = PropertyAccessorHelper.fromSourceToTargetClass(relationMetadata
                                        .getIdAttribute().getJavaType(), String.class, colValue.getAsString());
                                relationValue.put(fieldName, colVal);
                            }
                        }
                    }
                }
            }
            if (relationValue != null && !relationValue.isEmpty())
            {
                EnhanceEntity e = new EnhanceEntity(entity, PropertyAccessorHelper.getId(entity, m), relationValue);
                return e;
            }
            else
            {
                return entity;
            }
        }
        catch (Exception e)
        {
            log.error("Error while extracting entity object from json, caused by {}.", e);
            throw new KunderaException(e);
        }
    }

    private static void setFieldValue(Object entity, Attribute column, JsonElement value)
    {
        if (column.getJavaType().isAssignableFrom(byte[].class))
        {
            PropertyAccessorHelper.set(entity, (Field) column.getJavaMember(),
                    PropertyAccessorFactory.STRING.toBytes(value.getAsString()));
        }
        else
        {
            PropertyAccessorHelper.set(entity, (Field) column.getJavaMember(), PropertyAccessorHelper
                    .fromSourceToTargetClass(column.getJavaType(), String.class, value.getAsString()));
        }
    }
    
    /**
     * @param entityType
     * @param column
     * @param m
     * @param entity
     * @param embeddable
     * @param document
     */
    private static void onViaEmbeddable(EntityType entityType, Attribute column, EntityMetadata m, Object entity,
            EmbeddableType embeddable, JsonObject jsonObj)
    {
        Field embeddedField = (Field) column.getJavaMember();
        JsonElement embeddedDocumentObject = jsonObj.get(((AbstractAttribute) column).getJPAColumnName());
        if (!column.isCollection())
        {
            PropertyAccessorHelper.set(
                    entity,
                    embeddedField,
                    getObjectFromJson(embeddedDocumentObject.getAsJsonObject(),
                            ((AbstractAttribute) column).getBindableJavaType(), embeddable.getAttributes()));
        }
    }

    /**
     * 
     * @param jsonObj
     * @param clazz
     * @param columns
     * @return
     */
    private static Object getObjectFromJson(JsonObject jsonObj, Class clazz, Set<Attribute> columns)
    {
        try
        {
            Object obj = clazz.newInstance();
            for (Attribute column : columns)
            {
                JsonElement value = jsonObj.get(((AbstractAttribute) column).getJPAColumnName());
                setFieldValue(obj, column, value);
            }
            return obj;
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

    /**
     * 
     * @param value
     * @param clazz
     * @return
     */
    private static JsonElement getJsonPrimitive(Object value, Class clazz)
    {
        if (value != null)
        {
            if (clazz.isAssignableFrom(Number.class) || value instanceof Number)
            {
                return new JsonPrimitive((Number) value);
            }
            else if (clazz.isAssignableFrom(Boolean.class) || value instanceof Boolean)
            {
                return new JsonPrimitive((Boolean) value);
            }
            else if (clazz.isAssignableFrom(Character.class) || value instanceof Character)
            {
                return new JsonPrimitive((Character) value);
            }
            else if (clazz.isAssignableFrom(byte[].class) || value instanceof byte[])
            {
                return new JsonPrimitive(PropertyAccessorFactory.STRING.fromBytes(String.class, (byte[]) value));
            }
            else
            {
                return new JsonPrimitive(PropertyAccessorHelper.getString(value));
            }
        }
        return null;
    }
}