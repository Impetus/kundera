/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.client.cassandra.dsdriver;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.impetus.client.cassandra.schemamanager.CassandraDataTranslator;
import com.impetus.client.cassandra.schemamanager.CassandraDataTranslator.CassandraType;
import com.impetus.client.cassandra.schemamanager.CassandraValidationClassMapper;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.CharAccessor;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Utility class.
 * 
 * @author vivek.mishra
 * 
 */
public final class DSClientUtilities
{

    /**
     * assign value to provided entity instance else return value of mapped java
     * type.
     * 
     * @param row
     *            DS row
     * @param entity
     *            JPA entity
     * @param metadata
     *            entity's metadata
     * @param dataType
     *            data type
     * @param entityType
     *            entity type from metamodel
     * @param columnName
     *            jpa column name
     * @param member
     *            the member
     * @param metamodel
     *            the metamodel
     * @return modified entity instance with data type value. If entity is null
     *         then returns value of mapped java class.
     */
    @SuppressWarnings("unchecked")
    static Object assign(Row row, Object entity, EntityMetadata metadata, Name dataType, EntityType entityType,
            String columnName, Field member, MetamodelImpl metamodel)
    {
        String fieldName = null;

        // if metadata is null or it is relational attribute do not set
        // fieldName and member.

        if (metadata != null)
        {
            if (columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
            {
                entity = KunderaCoreUtils.initialize(metadata, entity, null);
                fieldName = metadata.getIdAttribute().getName();
                member = (Field) metadata.getIdAttribute().getJavaMember();
            }
            else if (metadata.getRelationNames() == null || !metadata.getRelationNames().contains(columnName))
            {
                fieldName = metadata.getFieldName(columnName);
                if (fieldName != null && entityType != null)
                {
                    entity = KunderaCoreUtils.initialize(metadata, entity, null);
                    member = (Field) entityType.getAttribute(fieldName).getJavaMember();
                }
            }
        }

        // Field member=null;
        /*
         * if (metadata != null && (metadata.getRelationNames() == null ||
         * !metadata.getRelationNames().contains(columnName))) { if
         * (!columnName.equals(((AbstractAttribute)
         * metadata.getIdAttribute()).getJPAColumnName())) { fieldName =
         * metadata.getFieldName(columnName); if (fieldName != null &&
         * entityType != null) { entity =
         * CassandraUtilities.initialize(metadata, entity, null); member =
         * (Field) entityType.getAttribute(fieldName).getJavaMember(); } } else
         * { entity = CassandraUtilities.initialize(metadata, entity, null);
         * fieldName = metadata.getIdAttribute().getName(); member = (Field)
         * metadata.getIdAttribute().getJavaMember(); }
         * 
         * }
         */
        Object retVal = null;
        
        if(row.isNull(columnName)){
        	return entity;
        }
        
        switch (dataType)
        {
        case BLOB:
        case CUSTOM:
            retVal = row.getBytes(columnName);
            if (member != null && retVal != null && entity != null)
            {
                PropertyAccessorHelper.set(entity, member, ((ByteBuffer) retVal).array());
            }
            break;

        case BOOLEAN:
            retVal = row.getBool(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case BIGINT:
        case COUNTER:
            retVal = row.getLong(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case DECIMAL:
            retVal = row.getDecimal(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case DOUBLE:
            retVal = row.getDouble(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case FLOAT:
            retVal = row.getFloat(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case INET:
            retVal = row.getInet(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case INT:
            retVal = row.getInt(columnName);
            retVal = setIntValue(member, retVal);
            setFieldValue(entity, member, retVal);
            break;

        case ASCII:
        case TEXT:
        case VARCHAR:
            retVal = row.getString(columnName);
            retVal = setTextValue(entity, member, retVal);
            setFieldValue(entity, member, retVal);
            break;

        case TIMESTAMP:
            retVal = row.getTimestamp(columnName);
            if (retVal != null && member != null)
                retVal = CassandraDataTranslator.decompose(member.getType(),
                        ByteBufferUtil.bytes(((Date) retVal).getTime()).array(), true);
            setFieldValue(entity, member, retVal);
            break;

        case VARINT:
            retVal = row.getVarint(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case UUID:
        case TIMEUUID:
            retVal = row.getUUID(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case LIST:
            Class listAttributeTypeClass = PropertyAccessorHelper.getGenericClass(member);
            Class listClazz = null;
            boolean isElementCollectionList = false;
            if (listAttributeTypeClass.isAssignableFrom(byte[].class))
            {

                listClazz = ByteBuffer.class;
            }
            else if (listAttributeTypeClass.isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionList = true;
                listClazz = UDTValue.class;
            }
            else
            {
                listClazz = listAttributeTypeClass;
            }
            retVal = row.getList(columnName, listClazz);
            Collection resultList = new ArrayList();
            if (isElementCollectionList)
            {
                Iterator collectionItems = ((Collection) retVal).iterator();
                while (collectionItems.hasNext())
                {
                    resultList.add(setUDTValue(entity, listAttributeTypeClass, (UDTValue) collectionItems.next(),
                            metamodel));
                }
            }

            if (retVal != null && !((List) retVal).isEmpty() && !isElementCollectionList)
            {
                if (listAttributeTypeClass.isAssignableFrom(byte[].class))
                {
                    setFieldValue(entity, member, CassandraDataTranslator.marshalCollection(BytesType.class,
                            (Collection) retVal, listAttributeTypeClass, ArrayList.class));
                }
                else
                {
                    Iterator collectionItems = ((Collection) retVal).iterator();
                    while (collectionItems.hasNext())
                    {
                        resultList.add(collectionItems.next());
                    }
                    setFieldValue(entity, member, resultList);
                }

            }
            else if (retVal != null && !((Collection) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultList);
            }
            break;

        case SET:
            Class setAttributeTypeClass = PropertyAccessorHelper.getGenericClass(member);
            Class setClazz = null;
            boolean isElementCollectionSet = false;
            if (setAttributeTypeClass.isAssignableFrom(byte[].class))
            {

                setClazz = ByteBuffer.class;
            }
            else if (setAttributeTypeClass.isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionSet = true;
                setClazz = UDTValue.class;
            }
            else
            {
                setClazz = setAttributeTypeClass;
            }
            retVal = row.getSet(columnName, setClazz);
            Collection resultSet = new HashSet();
            if (isElementCollectionSet)
            {
                Iterator collectionItems = ((Collection) retVal).iterator();
                while (collectionItems.hasNext())
                {
                    resultSet.add(setUDTValue(entity, setAttributeTypeClass, (UDTValue) collectionItems.next(),
                            metamodel));
                }
            }

            if (retVal != null && !((Set) retVal).isEmpty() && !isElementCollectionSet)
            {
                if (setAttributeTypeClass.isAssignableFrom(byte[].class))
                {
                    setFieldValue(entity, member, CassandraDataTranslator.marshalCollection(BytesType.class,
                            (Collection) retVal, setAttributeTypeClass, HashSet.class));
                }
                else
                {
                    Iterator collectionItems = ((Collection) retVal).iterator();
                    while (collectionItems.hasNext())
                    {
                        resultSet.add(collectionItems.next());
                    }
                    setFieldValue(entity, member, resultSet);
                }
            }
            else if (retVal != null && !((Collection) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultSet);
            }
            break;
        /*
         * ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT, INET,
         * INT, TEXT, TIMESTAMP, UUID, VARCHAR, VARINT, TIMEUUID, LIST, SET,
         * MAP, CUSTOM;
         */
        case MAP:
            List<Class<?>> mapGenericClasses = PropertyAccessorHelper.getGenericClasses(member);
            
            if(mapGenericClasses.isEmpty()){
            	//TODO: get map types from column metadata where member is null (Scalar queries)
            	break;
            }

            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(mapGenericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper
                    .getValidationClassInstance(mapGenericClasses.get(1), true);
            Class mapValueClazz = null;
            boolean isElementCollectionMap = false;
            if (mapGenericClasses.get(1).isAssignableFrom(byte[].class))
            {

                mapValueClazz = ByteBuffer.class;
            }
            else if (mapGenericClasses.get(1).isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionMap = true;
                mapValueClazz = UDTValue.class;
            }
            else
            {
                mapValueClazz = mapGenericClasses.get(1);
            }
            retVal = row.getMap(columnName, mapGenericClasses.get(0).isAssignableFrom(byte[].class) ? ByteBuffer.class
                    : mapGenericClasses.get(0), mapValueClazz);

            Map resultMap = new HashMap();

            if (isElementCollectionMap)
            {
                Iterator keys = ((Map) retVal).keySet().iterator();
                while (keys.hasNext())
                {
                    Object keyValue = keys.next();
                    resultMap.put(
                            keyValue,
                            setUDTValue(entity, mapGenericClasses.get(1), (UDTValue) ((Map) retVal).get(keyValue),
                                    metamodel));
                }
            }

            boolean isByteBuffer = mapGenericClasses.get(0).isAssignableFrom(byte[].class)
                    || mapGenericClasses.get(1).isAssignableFrom(byte[].class);

            // set the values.
            if (retVal != null && !((Map) retVal).isEmpty() && !isElementCollectionMap)
            {
                if (isByteBuffer)
                {
                    setFieldValue(entity, member,
                            CassandraDataTranslator.marshalMap(mapGenericClasses, keyClass, valueClass, (Map) retVal));
                }
                else
                {
                    Iterator keys = ((Map) retVal).keySet().iterator();
                    while (keys.hasNext())
                    {
                        Object keyValue = keys.next();
                        resultMap.put(keyValue, ((Map) retVal).get(keyValue));
                    }
                    setFieldValue(entity, member, resultMap);
                }
            }
            else if (retVal != null && !((Map) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultMap);
            }
            break;
        case UDT:
            retVal = row.getUDTValue(columnName);
            setFieldValue(entity, member,
                    retVal != null ? setUDTValue(entity, member.getType(), (UDTValue) retVal, metamodel) : null);
            break;
        }

        return entity != null ? entity : retVal;
    }

    /**
     * Sets the udt value.
     * 
     * @param entity
     *            the entity
     * @param embeddedClass
     *            the embedded class
     * @param udt
     *            the udt
     * @param metaModel
     *            the meta model
     * @return the object
     */
    private static Object setUDTValue(Object entity, Class embeddedClass, UDTValue udt, MetamodelImpl metaModel)
    {
        Object embeddedObject = KunderaCoreUtils.createNewInstance(embeddedClass);
        EmbeddableType embeddable = metaModel.embeddable(embeddedClass);

        for (Object subAttribute : embeddable.getAttributes())
        {
            Field embeddableColumn = (Field) ((AbstractAttribute) subAttribute).getJavaMember();
            if (metaModel.isEmbeddable(embeddableColumn.getType()))
            {
                UDTValue subUDT = udt.getUDTValue(((AbstractAttribute) subAttribute).getJPAColumnName());

                setFieldValue(embeddedObject, embeddableColumn,
                        setUDTValue(embeddedObject, embeddableColumn.getType(), subUDT, metaModel));
            }
            else
            {
                setBasicValue(embeddedObject, embeddableColumn, ((AbstractAttribute) subAttribute).getJPAColumnName(),
                        udt, CassandraDataTranslator.getCassandraDataTypeClass(embeddableColumn.getType()), metaModel);
            }

        }
        return embeddedObject;

    }

    /**
     * Sets the basic value.
     * 
     * @param entity
     *            the entity
     * @param member
     *            the member
     * @param columnName
     *            the column name
     * @param row
     *            the row
     * @param dataType
     *            the data type
     * @param metamodel
     *            the metamodel
     */
    private static void setBasicValue(Object entity, Field member, String columnName, UDTValue row,
            CassandraType dataType, MetamodelImpl metamodel)
    {
    	if(row.isNull(columnName)){
        	return;
        }
        Object retVal = null;
        switch (dataType)
        {
        case BYTES:
            // case CUSTOM:
            retVal = row.getBytes(columnName);
            if (retVal != null)
            {
                setFieldValue(entity, member, ((ByteBuffer) retVal).array());
            }
            break;

        case BOOLEAN:
            retVal = row.getBool(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case BIGINT:
            // bigints in embeddables and element collections are mapped/defined
            // by Long
        case LONG:
        case COUNTER:
            retVal = row.getLong(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case DECIMAL:
            retVal = row.getDecimal(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case DOUBLE:
            retVal = row.getDouble(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case FLOAT:
            retVal = row.getFloat(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case INET:
            retVal = row.getInet(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case INT:
            retVal = row.getInt(columnName);
            retVal = setIntValue(member, retVal);
            setFieldValue(entity, member, retVal);
            break;

        case ASCII:
        case STRING:
        case CHARACTER:
            try
            {
                row.getBytes(columnName);
            }
            catch (Exception e)
            {
                // do nothing
            }
            retVal = row.getString(columnName);
            retVal = setTextValue(entity, member, retVal);
            setFieldValue(entity, member, retVal);
            break;

        case TIMESTAMP:
            retVal = row.getTimestamp(columnName);
            if (retVal != null && member != null)
                retVal = CassandraDataTranslator.decompose(member.getType(),
                        ByteBufferUtil.bytes(((Date) retVal).getTime()).array(), true);
            setFieldValue(entity, member, retVal);
            break;

        case UUID:
            // case TIMEUUID:
            retVal = row.getUUID(columnName);
            setFieldValue(entity, member, retVal);
            break;

        case LIST:
            Class listAttributeTypeClass = PropertyAccessorHelper.getGenericClass(member);
            Class listClazz = null;
            boolean isElementCollectionList = false;
            if (listAttributeTypeClass.isAssignableFrom(byte[].class))
            {

                listClazz = ByteBuffer.class;
            }
            else if (listAttributeTypeClass.isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionList = true;
                listClazz = UDTValue.class;
            }
            else
            {
                listClazz = listAttributeTypeClass;
            }
            retVal = row.getList(columnName, listClazz);
            Collection resultList = new ArrayList();
            if (isElementCollectionList)
            {
                Iterator collectionItems = ((Collection) retVal).iterator();
                while (collectionItems.hasNext())
                {
                    resultList.add(setUDTValue(entity, listAttributeTypeClass, (UDTValue) collectionItems.next(),
                            metamodel));
                }
            }

            if (retVal != null && !((List) retVal).isEmpty() && !isElementCollectionList)
            {
                if (listAttributeTypeClass.isAssignableFrom(byte[].class))
                {
                    setFieldValue(entity, member, CassandraDataTranslator.marshalCollection(BytesType.class,
                            (Collection) retVal, listAttributeTypeClass, ArrayList.class));
                }
                else
                {
                    Iterator collectionItems = ((Collection) retVal).iterator();
                    while (collectionItems.hasNext())
                    {
                        resultList.add(collectionItems.next());
                    }
                    setFieldValue(entity, member, resultList);
                }

            }
            else if (retVal != null && !((Collection) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultList);
            }
            break;

        case SET:
            Class setAttributeTypeClass = PropertyAccessorHelper.getGenericClass(member);
            Class setClazz = null;
            boolean isElementCollectionSet = false;
            if (setAttributeTypeClass.isAssignableFrom(byte[].class))
            {

                setClazz = ByteBuffer.class;
            }
            else if (setAttributeTypeClass.isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionSet = true;
                setClazz = UDTValue.class;
            }
            else
            {
                setClazz = setAttributeTypeClass;
            }
            retVal = row.getSet(columnName, setClazz);
            Collection resultSet = new HashSet();
            if (isElementCollectionSet)
            {
                Iterator collectionItems = ((Collection) retVal).iterator();
                while (collectionItems.hasNext())
                {
                    resultSet.add(setUDTValue(entity, setAttributeTypeClass, (UDTValue) collectionItems.next(),
                            metamodel));
                }
            }

            if (retVal != null && !((Set) retVal).isEmpty() && !isElementCollectionSet)
            {
                if (setAttributeTypeClass.isAssignableFrom(byte[].class))
                {
                    setFieldValue(entity, member, CassandraDataTranslator.marshalCollection(BytesType.class,
                            (Collection) retVal, setAttributeTypeClass, HashSet.class));
                }
                else
                {
                    Iterator collectionItems = ((Collection) retVal).iterator();
                    while (collectionItems.hasNext())
                    {
                        resultSet.add(collectionItems.next());
                    }
                    setFieldValue(entity, member, resultSet);
                }
            }
            else if (retVal != null && !((Collection) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultSet);
            }
            break;
        /*
         * ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT, INET,
         * INT, TEXT, TIMESTAMP, UUID, VARCHAR, VARINT, TIMEUUID, LIST, SET,
         * MAP, CUSTOM;
         */
        case MAP:
            List<Class<?>> mapGenericClasses = PropertyAccessorHelper.getGenericClasses(member);

            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(mapGenericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper
                    .getValidationClassInstance(mapGenericClasses.get(1), true);
            Class mapValueClazz = null;
            boolean isElementCollectionMap = false;
            if (mapGenericClasses.get(1).isAssignableFrom(byte[].class))
            {

                mapValueClazz = ByteBuffer.class;
            }
            else if (mapGenericClasses.get(1).isAnnotationPresent(Embeddable.class))
            {
                isElementCollectionMap = true;
                mapValueClazz = UDTValue.class;
            }
            else
            {
                mapValueClazz = mapGenericClasses.get(1);
            }
            retVal = row.getMap(columnName, mapGenericClasses.get(0).isAssignableFrom(byte[].class) ? ByteBuffer.class
                    : mapGenericClasses.get(0), mapValueClazz);

            Map resultMap = new HashMap();

            if (isElementCollectionMap)
            {
                Iterator keys = ((Map) retVal).keySet().iterator();
                while (keys.hasNext())
                {
                    Object keyValue = keys.next();
                    resultMap.put(
                            keyValue,
                            setUDTValue(entity, mapGenericClasses.get(1), (UDTValue) ((Map) retVal).get(keyValue),
                                    metamodel));
                }
            }

            boolean isByteBuffer = mapGenericClasses.get(0).isAssignableFrom(byte[].class)
                    || mapGenericClasses.get(1).isAssignableFrom(byte[].class);

            // set the values.
            if (retVal != null && !((Map) retVal).isEmpty() && !isElementCollectionMap)
            {
                if (isByteBuffer)
                {
                    setFieldValue(entity, member,
                            CassandraDataTranslator.marshalMap(mapGenericClasses, keyClass, valueClass, (Map) retVal));
                }
                else
                {
                    Iterator keys = ((Map) retVal).keySet().iterator();
                    while (keys.hasNext())
                    {
                        Object keyValue = keys.next();
                        resultMap.put(keyValue, ((Map) retVal).get(keyValue));
                    }
                    setFieldValue(entity, member, resultMap);
                }
            }
            else if (retVal != null && !((Map) retVal).isEmpty())
            {
                setFieldValue(entity, member, resultMap);
            }
            break;
        }

    }

    /**
     * Sets the int value.
     * 
     * @param member
     *            the member
     * @param retVal
     *            the ret val
     * @return the object
     */
    private static Object setIntValue(Field member, Object retVal)
    {
        if (member != null)
        {
            if (member.getType().isAssignableFrom(byte.class))
            {
                retVal = ((Integer) retVal).byteValue();
            }
            else if (member.getType().isAssignableFrom(short.class))
            {
                retVal = ((Integer) retVal).shortValue();
            }
        }
        return retVal;
    }

    /**
     * Sets the text value.
     * 
     * @param entity
     *            the entity
     * @param member
     *            the member
     * @param retVal
     *            the ret val
     * @return the object
     */
    private static Object setTextValue(Object entity, Field member, Object retVal)
    {
        if (member != null && member.getType().isEnum())
        {
            EnumAccessor accessor = new EnumAccessor();
            if (member != null)
            {
                retVal = accessor.fromString(member.getType(), (String) retVal);
            }
        }
        else if (member != null
                && (member.getType().isAssignableFrom(char.class) || member.getType().isAssignableFrom(Character.class)))
        {
            retVal = new CharAccessor().fromString(member.getType(), (String) retVal);
        }
        return retVal;
    }

    /**
     * Sets the field value.
     * 
     * @param entity
     *            the entity
     * @param member
     *            the member
     * @param retVal
     *            the ret val
     */
    private static void setFieldValue(Object entity, Field member, Object retVal)
    {
        if (member != null && retVal != null && entity != null)
        {
            PropertyAccessorHelper.set(entity, member, retVal);
        }
    }

}
