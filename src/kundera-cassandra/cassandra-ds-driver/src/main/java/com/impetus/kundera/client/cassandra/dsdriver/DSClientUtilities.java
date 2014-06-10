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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.schemamanager.CassandraDataTranslator;
import com.impetus.client.cassandra.schemamanager.CassandraValidationClassMapper;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.CharAccessor;
import com.impetus.kundera.property.accessor.EnumAccessor;

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
     * @return modified entity instance with data type value. If entity is null
     *         then returns value of mapped java class.
     */
    static Object assign(Row row, Object entity, EntityMetadata metadata, Name dataType, EntityType entityType,
            String columnName, Field member)
    {
        String fieldName = null;
        
        // if metadata is null or it is relational attribute do not set fieldName and member.
        
        if(metadata != null)
        {
            if(columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
            {
                entity = CassandraUtilities.initialize(metadata, entity, null);
                fieldName = metadata.getIdAttribute().getName();
                member = (Field) metadata.getIdAttribute().getJavaMember();
            } else if(metadata.getRelationNames() == null || !metadata.getRelationNames().contains(columnName))
            {
                fieldName = metadata.getFieldName(columnName);
                if (fieldName != null && entityType != null)
                {
                    entity = CassandraUtilities.initialize(metadata, entity, null);
                    member = (Field) entityType.getAttribute(fieldName).getJavaMember();
                }
            }
        }
        
        // Field member=null;
     /*   if (metadata != null
                && (metadata.getRelationNames() == null || !metadata.getRelationNames().contains(columnName)))
        {
            if (!columnName.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
            {
                fieldName = metadata.getFieldName(columnName);
                if (fieldName != null && entityType != null)
                {
                    entity = CassandraUtilities.initialize(metadata, entity, null);
                    member = (Field) entityType.getAttribute(fieldName).getJavaMember();
                }
            }
            else
            {
                entity = CassandraUtilities.initialize(metadata, entity, null);
                fieldName = metadata.getIdAttribute().getName();
                member = (Field) metadata.getIdAttribute().getJavaMember();
            }

        }*/
        Object retVal = null;

        switch (dataType)
        {
        case BLOB:
        case CUSTOM:
            retVal = row.getBytes(columnName);
            if (retVal != null)
            {
                PropertyAccessorHelper.set(entity, member, ((ByteBuffer) retVal).array());
                // setFieldValue(entity, member, retVal);
            }
            break;

        case BOOLEAN:
            retVal = row.getBool(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
            break;

        case BIGINT:
        case COUNTER:
            retVal = row.getLong(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
            break;

        case DECIMAL:
            retVal = row.getDecimal(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
            break;

        case DOUBLE:
            retVal = row.getDouble(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
            break;

        case FLOAT:
            retVal = row.getFloat(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
            break;

        case INET:
            retVal = row.getInet(columnName);
            setFieldValue(entity, member, retVal);
            // PropertyAccessorHelper.set(entity, member, retVal);
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
            retVal = row.getDate(columnName);
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
            retVal = row.getList(columnName, listAttributeTypeClass.isAssignableFrom(byte[].class)? ByteBuffer.class:listAttributeTypeClass);

            if(retVal != null && !((List)retVal).isEmpty())
            PropertyAccessorHelper.set(
                    entity,
                    member,
                    listAttributeTypeClass.isAssignableFrom(byte[].class) ? CassandraDataTranslator.marshalCollection(
                            BytesType.class, (Collection) retVal, listAttributeTypeClass, ArrayList.class) : retVal);
            break;

        case SET:
            Class setAttributeTypeClass = PropertyAccessorHelper.getGenericClass(member);
//            retVal = row.getList(columnName, setAttributeTypeClass.isAssignableFrom(byte[].class)? ByteBuffer.class:setAttributeTypeClass);
            
            retVal = row.getSet(columnName, setAttributeTypeClass.isAssignableFrom(byte[].class)? ByteBuffer.class:setAttributeTypeClass);
            if(retVal != null && !((Set)retVal).isEmpty())
            PropertyAccessorHelper.set(
                    entity,
                    member,
                    setAttributeTypeClass.isAssignableFrom(byte[].class) ? CassandraDataTranslator.marshalCollection(
                            BytesType.class, (Collection) retVal, setAttributeTypeClass, HashSet.class) : retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
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
//            retVal = row.getMap(columnName, keyClass, valueClass);
            retVal = row.getMap(columnName,
                    mapGenericClasses.get(0).isAssignableFrom(byte[].class) ? ByteBuffer.class
                            : mapGenericClasses.get(0), /*
                                                         * mapGenericClasses.get(
                                                         * 0),
                                                         */
                    mapGenericClasses.get(1).isAssignableFrom(byte[].class) ? ByteBuffer.class
                            : mapGenericClasses.get(1));
            
            boolean isByteBuffer = mapGenericClasses.get(0).isAssignableFrom(byte[].class)
                    || mapGenericClasses.get(1).isAssignableFrom(byte[].class);  

            // set the values.
            if(retVal != null && !((Map)retVal).isEmpty())
            PropertyAccessorHelper.set(
                    entity,
                    member,
                    isByteBuffer ? CassandraDataTranslator.marshalMap(mapGenericClasses, keyClass, valueClass,
                            (Map) retVal) : retVal);
            break;
        }

        return entity != null ? entity : retVal;
    }

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

    private static void setFieldValue(Object entity, Field member, Object retVal)
    {
        if (member != null && retVal != null && entity != null)
        {
            PropertyAccessorHelper.set(entity, member, retVal);
        }
    }

}
