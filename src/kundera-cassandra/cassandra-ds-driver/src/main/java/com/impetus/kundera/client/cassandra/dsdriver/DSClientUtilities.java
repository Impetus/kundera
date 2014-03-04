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
import java.util.List;

import javax.persistence.metamodel.EntityType;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.DataType.Name;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.schemamanager.CassandraValidationClassMapper;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
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
            String columnName)
    {
        String fieldName = null;
        Field member=null;
        if(!columnName.equalsIgnoreCase("key"))
        {
            fieldName = metadata.getFieldName(columnName);
            if(fieldName !=null)
            {
                entity = CassandraUtilities.initialize(metadata, entity, null);
                member = (Field) entityType.getAttribute(fieldName).getJavaMember();
            }
       }else
       {
           entity = CassandraUtilities.initialize(metadata, entity, null);
           fieldName=metadata.getIdAttribute().getName();
           member = (Field) metadata.getIdAttribute().getJavaMember();
       }

        Object retVal = null;

        switch (dataType)
        {
        case BLOB:
        case CUSTOM:
            retVal = row.getBytes(columnName);
            if(retVal !=null)
            {
                PropertyAccessorHelper.set(entity, member, ((ByteBuffer)retVal).array());
//                setFieldValue(entity, member, retVal);
            }
            break;

        case BOOLEAN:
            retVal = row.getBool(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case BIGINT:
        case COUNTER:
            retVal = row.getLong(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case DECIMAL:
            retVal = row.getDecimal(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case DOUBLE:
            retVal = row.getDouble(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case FLOAT:
            retVal = row.getFloat(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case INET:
            retVal = row.getInet(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case INT:
            retVal = row.getInt(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case ASCII:
        case TEXT:
        case VARCHAR:
            retVal = row.getString(columnName);
            if(member.getType().isEnum())
            {
                EnumAccessor accessor = new EnumAccessor();
                if(member !=null)
                PropertyAccessorHelper.set(entity, member, accessor.fromString(member.getType(), (String) retVal));
            } else
            {
                setFieldValue(entity, member, retVal);
//                PropertyAccessorHelper.set(entity, member, retVal);
            }
            break;
            
        case TIMESTAMP:
            retVal = row.getDate(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case VARINT:
            retVal = row.getVarint(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case UUID:
        case TIMEUUID:
            retVal = row.getUUID(columnName);
            setFieldValue(entity, member, retVal);
//            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case LIST:
            retVal = row.getList(columnName, PropertyAccessorHelper.getGenericClass(member));
            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            
        case SET:
            retVal = row.getSet(columnName, PropertyAccessorHelper.getGenericClass(member));
            PropertyAccessorHelper.set(entity, member, retVal);
            break;
            /*
             * ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT,
             * INET, INT, TEXT, TIMESTAMP, UUID, VARCHAR, VARINT, TIMEUUID,
             * LIST, SET, MAP, CUSTOM;
             */
        case MAP:
            List<Class<?>> mapGenericClasses = PropertyAccessorHelper.getGenericClasses(member);

            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(mapGenericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper
                    .getValidationClassInstance(mapGenericClasses.get(1), true);
            retVal = row.getMap(columnName, keyClass, valueClass);

            PropertyAccessorHelper.set(entity, member, retVal);
            break;
        }

        return entity != null ? entity : retVal;
    }

    private static void setFieldValue(Object entity, Field member, Object retVal)
    {
        if(member !=null)
        {
            PropertyAccessorHelper.set(entity, member, retVal);
        }
    }

}
