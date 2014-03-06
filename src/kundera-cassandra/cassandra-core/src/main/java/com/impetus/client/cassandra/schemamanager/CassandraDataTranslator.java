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

package com.impetus.client.cassandra.schemamanager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.PersistenceException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.ByteAccessor;
import com.impetus.kundera.property.accessor.CalendarAccessor;
import com.impetus.kundera.property.accessor.CharAccessor;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.impetus.kundera.property.accessor.IntegerAccessor;
import com.impetus.kundera.property.accessor.SQLDateAccessor;
import com.impetus.kundera.property.accessor.SQLTimeAccessor;
import com.impetus.kundera.property.accessor.SQLTimestampAccessor;
import com.impetus.kundera.property.accessor.ShortAccessor;

/**
 * Class to decompose and compose Cassandra data type objects
 * 
 * @author chhavi.gangwal
 */

public final class CassandraDataTranslator
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(CassandraDataTranslator.class);

    /** The cassandra data type map. */
    private static Map<Class<?>, CassandraType> typeToClazz = new HashMap<Class<?>, CassandraDataTranslator.CassandraType>();

    /**
     * CassandradataType enum
     * 
     */
    static enum CassandraType
    {

        INT32(Int32Type.class.getSimpleName()), DOUBLE(DoubleType.class.getSimpleName()), FLOAT(FloatType.class
                .getSimpleName()), LONG(LongType.class.getSimpleName()), STRING(UTF8Type.class.getSimpleName()), UUID(
                UUIDType.class.getSimpleName()), BYTES(BytesType.class.getSimpleName()), ASCII(AsciiType.class
                .getSimpleName()), BOOLEAN(BooleanType.class.getSimpleName()), DATE(DateType.class.getSimpleName()), INT(
                IntegerType.class.getSimpleName()), DECIMAL(DecimalType.class.getSimpleName()), COUNTER(
                CounterColumnType.class.getSimpleName()), LIST(ListType.class.getSimpleName()), MAP(MapType.class
                .getSimpleName()), SET(SetType.class.getSimpleName()), INET(InetAddressType.class.getSimpleName()), SHORT(
                Short.class.getSimpleName()), ENUM(Enum.class.getSimpleName()), CHARACTER(UTF8Type.class
                .getSimpleName()), BIGINT(IntegerType.class.getSimpleName()), TIMESTAMP(DateType.class.getSimpleName()), SQL_DATE(
                DateType.class.getSimpleName()), SQL_TIME(DateType.class.getSimpleName()), SQL_TIMESTAMP(DateType.class
                .getSimpleName()), CALENDAR(DateType.class.getSimpleName());

        private String clazz;

        private static final Map<String, CassandraType> lookup = new HashMap<String, CassandraType>();

        static
        {
            for (CassandraType s : EnumSet.allOf(CassandraType.class))
                lookup.put(s.getClazz(), s);
        }

        /**
         * @param clazz
         */
        private CassandraType(String clazz)
        {
            this.clazz = clazz;
        }

        /**
         * @return
         */
        public String getClazz()
        {
            return clazz;
        }

        /**
         * @param clazz
         * @return
         */
        public static CassandraType get(String clazz)
        {
            return lookup.get(clazz);
        }
    }

    static
    {
        typeToClazz.put(java.lang.String.class, CassandraType.STRING);

        typeToClazz.put(Character.class, CassandraType.CHARACTER);
        typeToClazz.put(char.class, CassandraType.CHARACTER);

        typeToClazz.put(java.lang.Integer.class, CassandraType.INT);
        typeToClazz.put(int.class, CassandraType.INT);

        typeToClazz.put(Short.class, CassandraType.SHORT);
        typeToClazz.put(short.class, CassandraType.SHORT);

        typeToClazz.put(java.math.BigDecimal.class, CassandraType.DECIMAL);

        typeToClazz.put(java.math.BigInteger.class, CassandraType.BIGINT);

        typeToClazz.put(java.lang.Double.class, CassandraType.DOUBLE);
        typeToClazz.put(double.class, CassandraType.DOUBLE);

        typeToClazz.put(Calendar.class, CassandraType.CALENDAR);
        typeToClazz.put(java.sql.Date.class, CassandraType.SQL_DATE);
        typeToClazz.put(java.util.Date.class, CassandraType.DATE);
        typeToClazz.put(java.sql.Time.class, CassandraType.SQL_TIME);
        typeToClazz.put(java.sql.Timestamp.class, CassandraType.SQL_TIMESTAMP);

        typeToClazz.put(boolean.class, CassandraType.BOOLEAN);
        typeToClazz.put(Boolean.class, CassandraType.BOOLEAN);

        typeToClazz.put(java.lang.Long.class, CassandraType.LONG);
        typeToClazz.put(long.class, CassandraType.LONG);

        typeToClazz.put(Byte.class, CassandraType.BYTES);
        typeToClazz.put(byte.class, CassandraType.BYTES);

        typeToClazz.put(Float.class, CassandraType.FLOAT);
        typeToClazz.put(float.class, CassandraType.FLOAT);

        typeToClazz.put(UUID.class, CassandraType.UUID);

        typeToClazz.put(List.class, CassandraType.LIST);
        typeToClazz.put(Set.class, CassandraType.SET);
        typeToClazz.put(Map.class, CassandraType.MAP);

        typeToClazz.put(CounterColumn.class, CassandraType.COUNTER);
    }

    /**
     * @param clazz
     * @return
     */
    public static CassandraType getCassandraDataTypeClass(Class clazz)
    {

        return typeToClazz.get(clazz);
    }

    /**
     * Checks whether the data type object is that of cassandra
     * 
     * @param clazz
     * @return
     */
    public static boolean isCassandraDataTypeClass(Class clazz)
    {

        if (typeToClazz.get(clazz) == null)
        {
            return false;
        }
        return true;
    }

    /**
     * Calls compose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param isCql3Enabled
     * @return
     */
    public static byte[] compose(Class<?> dataTypeClazz, Object dataValue, boolean isCql3Enabled)
    {

        if (dataValue != null)
        {

            switch (getCassandraDataTypeClass(dataTypeClazz))
            {
            case INT:
                return IntegerTypeBuilder.compose(dataValue, dataTypeClazz);
            case DATE:
                return DateTypeBuilder.compose(dataValue, dataTypeClazz);
            case FLOAT:
                return FloatTypeBuilder.compose(dataValue, dataTypeClazz);
            case LONG:
                return LongTypeBuilder.compose(dataValue, dataTypeClazz);
            case DOUBLE:
                return DoubleTypeBuilder.compose(dataValue, dataTypeClazz);
            case STRING:
                return StringTypeBuilder.compose(dataValue, dataTypeClazz);
            case ASCII:
                return AsciiTypeBuilder.compose(dataValue, dataTypeClazz);
            case UUID:
                return UUIDTypeBuilder.compose(dataValue, dataTypeClazz);
            case BYTES:
                return BytesTypeBuilder.compose(dataValue, dataTypeClazz);
            case BOOLEAN:
                return BooleanTypeBuilder.compose(dataValue, dataTypeClazz);
            case INT32:
                return IntegerTypeBuilder.compose(dataValue, dataTypeClazz);
            case DECIMAL:
                return DecimalTypeBuilder.compose(dataValue, dataTypeClazz);
            case COUNTER:
                return CounterTypeBuilder.compose(dataValue, dataTypeClazz);
            case ENUM:
                return EnumTypeBuilder.compose(dataValue, dataTypeClazz);
            case CHARACTER:
                return CharacterTypeBuilder.compose(dataValue, dataTypeClazz);
            case SHORT:
                return ShortTypeBuilder.compose(dataValue, dataTypeClazz);
            case BIGINT:
                return BigIntegerTypeBuilder.compose(dataValue, dataTypeClazz);
            case TIMESTAMP:
                return TimeStampTypeBuilder.compose(dataValue, dataTypeClazz);
            case SQL_DATE:
                return SQLDateTypeBuilder.compose(dataValue, dataTypeClazz);
            case SQL_TIME:
                return SQLTimeTypeBuilder.compose(dataValue, dataTypeClazz);
            case SQL_TIMESTAMP:
                return SQLTimeStampTypeBuilder.compose(dataValue, dataTypeClazz);
            case CALENDAR:
                return CalendarTypeBuilder.compose(dataValue, dataTypeClazz);
            }
        }
        return null;
    }

    /**
     * Calls compose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param mapGenericClassses
     * @param isCql3Enabled
     * @return
     */
    public static byte[] compose(Class<?> dataTypeClazz, Object dataValue, List<Class<?>> mapGenericClassses,
            boolean isCql3Enabled)
    {
        if (dataValue != null)
        {
            if (dataTypeClazz.isAssignableFrom(Map.class))
            {
                return MapTypeBuilder.compose(dataValue, mapGenericClassses);
            }
        }
        return null;
    }

    /**
     * Calls compose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param mapGenericClassses
     * @param isCql3Enabled
     * @return
     */
    public static byte[] compose(Class<?> dataTypeClazz, Object dataValue, Class<?> mapGenericClassses,
            boolean isCql3Enabled)
    {
        if (dataValue != null)
        {
            switch (getCassandraDataTypeClass(dataTypeClazz))
            {
            case LIST:
                return ListTypeBuilder.compose(dataValue, dataTypeClazz, mapGenericClassses);
            case SET:
                return SetTypeBuilder.compose(dataValue, dataTypeClazz, mapGenericClassses);
            }
        }
        return null;
    }

    /**
     * Calls decompose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param isCql3Enabled
     * @return
     */
    public static Object decompose(Class<?> dataTypeClazz, Object dataValue, boolean isCql3Enabled)
    {

        if (dataValue != null)
        {

            switch (getCassandraDataTypeClass(dataTypeClazz))
            {
            case INT:
                return IntegerTypeBuilder.decompose(dataValue, dataTypeClazz);
            case DATE:
                return DateTypeBuilder.decompose(dataValue, dataTypeClazz);
            case FLOAT:
                return FloatTypeBuilder.decompose(dataValue, dataTypeClazz);
            case LONG:
                return LongTypeBuilder.decompose(dataValue, dataTypeClazz);
            case DOUBLE:
                return DoubleTypeBuilder.decompose(dataValue, dataTypeClazz);
            case STRING:
                return StringTypeBuilder.decompose(dataValue, dataTypeClazz);
            case ASCII:
                return AsciiTypeBuilder.decompose(dataValue, dataTypeClazz);
            case UUID:
                return UUIDTypeBuilder.decompose(dataValue, dataTypeClazz);
            case BYTES:
                if (isCql3Enabled)
                {
                    return BytesTypeBuilder.decompose(dataValue, dataTypeClazz);
                }
                else
                {
                    return BytesTypeBuilder.decomposeCQL2(dataValue, dataTypeClazz);
                }
            case BOOLEAN:
                return BooleanTypeBuilder.decompose(dataValue, dataTypeClazz);
            case INT32:
                return IntegerTypeBuilder.decompose(dataValue, dataTypeClazz);
            case DECIMAL:
                return DecimalTypeBuilder.decompose(dataValue, dataTypeClazz);
            case COUNTER:
                return CounterTypeBuilder.decompose(dataValue, dataTypeClazz);
            case ENUM:
                return EnumTypeBuilder.decompose(dataValue, dataTypeClazz);
            case CHARACTER:
                if (isCql3Enabled)
                {
                    return CharacterTypeBuilder.decompose(dataValue, dataTypeClazz);
                }
                else
                {
                    return CharacterTypeBuilder.decomposeCQL2(dataValue, dataTypeClazz);
                }
            case SHORT:
                if (isCql3Enabled)
                {
                    return ShortTypeBuilder.decompose(dataValue, dataTypeClazz);
                }
                else
                {
                    return ShortTypeBuilder.decomposeCQL2(dataValue, dataTypeClazz);
                }
            case BIGINT:
                return BigIntegerTypeBuilder.decompose(dataValue, dataTypeClazz);
            case TIMESTAMP:
                return TimeStampTypeBuilder.decompose(dataValue, dataTypeClazz);
            case SQL_DATE:
                return SQLDateTypeBuilder.decompose(dataValue, dataTypeClazz);
            case SQL_TIME:
                return SQLTimeTypeBuilder.decompose(dataValue, dataTypeClazz);
            case SQL_TIMESTAMP:
                return SQLTimeStampTypeBuilder.decompose(dataValue, dataTypeClazz);
            case CALENDAR:
                return CalendarTypeBuilder.decompose(dataValue, dataTypeClazz);
            }
        }
        return null;
    }

    /**
     * Calls decompose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param mapGenericClassses
     * @param isCql3Enabled
     * @return
     */
    public static Object decompose(Class<?> dataTypeClazz, Object dataValue, List<Class<?>> mapGenericClassses,
            boolean isCql3Enabled)
    {
        if (dataValue != null)
        {
            switch (getCassandraDataTypeClass(dataTypeClazz))
            {
            case MAP:
                return MapTypeBuilder.decompose(dataValue, mapGenericClassses);
            }
        }
        return null;
    }

    /**
     * Calls decompose function of respective data type of object
     * 
     * @param dataTypeClazz
     * @param dataValue
     * @param mapGenericClassses
     * @param isCql3Enabled
     * @return
     */
    public static Object decompose(Class<?> dataTypeClazz, Object dataValue, Class<?> mapGenericClassses,
            boolean isCql3Enabled)
    {
        if (dataValue != null)
        {
            switch (getCassandraDataTypeClass(dataTypeClazz))
            {
            case LIST:
                return ListTypeBuilder.decompose(dataValue, dataTypeClazz, mapGenericClassses);
            case SET:
                return SetTypeBuilder.decompose(dataValue, dataTypeClazz, mapGenericClassses);
            }
        }
        return null;
    }

    /**
     * marshal collection objects if its value is of type byte
     * 
     * @param cassandraTypeClazz
     * @param result
     * @param clazz
     * @return
     */
    private static Collection marshalCollection(Class cassandraTypeClazz, Collection result, Class clazz)
    {
        Collection mappedCollection = result;

        if (cassandraTypeClazz.isAssignableFrom(BytesType.class))
        {
            mappedCollection = (Collection) PropertyAccessorHelper.getObject(result.getClass());
            for (Object value : result)
            {
                mappedCollection.add(PropertyAccessorHelper.getObject(clazz, ((ByteBuffer) value).array()));
            }
        }
        return mappedCollection;
    }

    /**
     * Used to decompose and compose long type data objects
     * 
     */
    private static class LongTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return ByteBufferUtil.bytes((Long) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.toLong(buf);
        }
    }

    /**
     * Used to decompose and compose float type data objects
     * 
     */
    private static class FloatTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return ByteBufferUtil.bytes((Float) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.toFloat(buf);
        }
    }

    /**
     * Used to decompose and compose double type data objects
     * 
     */
    private static class DoubleTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return ByteBufferUtil.bytes((Double) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.toDouble(buf);
        }
    }

    /**
     * Used to decompose and compose string type data objects
     * 
     */
    private static class StringTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {

            return ByteBufferUtil.bytes((String) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            try
            {

                return ByteBufferUtil.string(buf);
            }
            catch (CharacterCodingException e)
            {
                log.warn("Error while setting field value, Caused by: .", e);
            }
            return null;
        }
    }

    /**
     * Used to decompose and compose bytes type data objects
     * 
     */
    private static class BytesTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            ByteAccessor accessor = new ByteAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            IntegerAccessor accessor = new IntegerAccessor();
            int thriftColumnValue = accessor.fromBytes(byte.class, (byte[]) value);
            ByteAccessor byteAccessor = new ByteAccessor();
            return byteAccessor.fromString(clazz, String.valueOf(thriftColumnValue));
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decomposeCQL2(Object value, Class clazz)
        {
            ByteAccessor byteAccessor = new ByteAccessor();
            return byteAccessor.fromBytes(clazz, (byte[]) value);
        }

    }

    /**
     * Used to decompose and compose decimal type data objects
     * 
     */
    private static class DecimalTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return DecimalType.instance.decompose((BigDecimal) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return DecimalType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose boolean type data objects
     * 
     */
    private static class BooleanTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return BooleanType.instance.decompose((Boolean) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return BooleanType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose UUID type data objects
     * 
     */
    private static class UUIDTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return UUIDType.instance.decompose((UUID) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return UUIDType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose ascii type data objects
     * 
     */
    private static class AsciiTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return AsciiType.instance.decompose((String) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return AsciiType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose integer type data objects
     * 
     */
    private static class IntegerTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return ByteBufferUtil.bytes((Integer) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.toInt(buf);
        }
    }

    /**
     * Used to decompose and compose set type data objects
     * 
     */
    private static class SetTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @param mapGenericClassses
         * @return
         */
        private static byte[] compose(Object value, Class clazz, Class<?> mapGenericClassses)
        {
            Class<?> valueValidationClass = CassandraValidationClassMapper.getValidationClassInstance(
                    mapGenericClassses, true);

            Object valueClassInstance;
            try
            {
                valueClassInstance = valueValidationClass.getDeclaredField("instance").get(null);
                SetType setType = SetType.getInstance((AbstractType) valueClassInstance);
                return setType.decompose((Set) value).array();
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }

        /**
         * @param value
         * @param clazz
         * @param mapGenericClassses
         * @return
         */
        private static Object decompose(Object value, Class clazz, Class<?> mapGenericClassses)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            Class<?> valueValidationClass = CassandraValidationClassMapper.getValidationClassInstance(
                    mapGenericClassses, true);

            Object valueClassInstance;
            try
            {
                valueClassInstance = valueValidationClass.getDeclaredField("instance").get(null);
                SetType setType = SetType.getInstance((AbstractType) valueClassInstance);
                Collection outputCollection = new HashSet();
                outputCollection.addAll(setType.compose(buf));
                return marshalCollection(valueValidationClass, outputCollection, mapGenericClassses);
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Used to decompose and compose map type data objects
     * 
     */
    private static class MapTypeBuilder
    {
        /**
         * @param value
         * @param mapGenericClasses
         * @return
         */
        private static byte[] compose(Object value, List<Class<?>> mapGenericClasses)
        {
            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(mapGenericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper
                    .getValidationClassInstance(mapGenericClasses.get(1), true);

            try
            {
                Object keyClassInstance = keyClass.getDeclaredField("instance").get(null);
                Object valueClassInstance = valueClass.getDeclaredField("instance").get(null);

                MapType mapType = MapType.getInstance((AbstractType) keyClassInstance,
                        (AbstractType) valueClassInstance);

                return mapType.decompose((Map) value).array();
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }

        /**
         * In case, key or value class is of type blob. Iterate and populate
         * corresponding byte[]
         * 
         * @param mapGenericClasses
         * @param keyClass
         * @param valueClass
         * @param rawMap
         * @return
         */
        private static Map marshalMap(List<Class<?>> mapGenericClasses, Class keyClass, Class valueClass, Map rawMap)
        {
            Map dataCollection = new HashMap();

            if (keyClass.isAssignableFrom(BytesType.class) || valueClass.isAssignableFrom(BytesType.class))
            {
                Iterator iter = rawMap.keySet().iterator();

                while (iter.hasNext())
                {

                    Object key = iter.next();
                    Object value = rawMap.get(key);

                    if (keyClass.isAssignableFrom(BytesType.class))
                    {
                        key = PropertyAccessorHelper.getObject(mapGenericClasses.get(0), ((ByteBuffer) key).array());
                    }

                    if (valueClass.isAssignableFrom(BytesType.class))
                    {
                        value = PropertyAccessorHelper
                                .getObject(mapGenericClasses.get(1), ((ByteBuffer) value).array());
                    }

                    dataCollection.put(key, value);
                }
            }
            return dataCollection;
        }

        /**
         * @param value
         * @param mapGenericClasses
         * @return
         */
        private static Object decompose(Object value, List<Class<?>> mapGenericClasses)
        {
            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(mapGenericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper
                    .getValidationClassInstance(mapGenericClasses.get(1), true);

            try
            {
                ByteBuffer valueByteBuffer = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);

                Object keyClassInstance = keyClass.getDeclaredField("instance").get(null);
                Object valueClassInstance = valueClass.getDeclaredField("instance").get(null);

                MapType mapType = MapType.getInstance((AbstractType) keyClassInstance,
                        (AbstractType) valueClassInstance);
                Map rawMap = new HashMap();
                rawMap.putAll(mapType.compose(valueByteBuffer));

                Map dataCollection = marshalMap(mapGenericClasses, keyClass, valueClass, rawMap);
                return dataCollection.isEmpty() ? rawMap : dataCollection;
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", keyClass.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Used to decompose and compose list type data objects
     * 
     */
    private static class ListTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @param mapGenericClassses
         * @return
         */
        private static byte[] compose(Object value, Class clazz, Class<?> mapGenericClassses)
        {
            Class<?> valueValidationClass = CassandraValidationClassMapper.getValidationClassInstance(
                    mapGenericClassses, true);

            Object valueClassInstance;
            try
            {
                valueClassInstance = valueValidationClass.getDeclaredField("instance").get(null);
                ListType listType = ListType.getInstance((AbstractType) valueClassInstance);
                return listType.decompose((List) value).array();
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }

        /**
         * @param value
         * @param clazz
         * @param mapGenericClassses
         * @return
         */
        private static Object decompose(Object value, Class clazz, Class<?> mapGenericClassses)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            Class<?> valueValidationClass = CassandraValidationClassMapper.getValidationClassInstance(
                    mapGenericClassses, true);
            Object valueClassInstance;
            try
            {
                valueClassInstance = valueValidationClass.getDeclaredField("instance").get(null);
                ListType listType = ListType.getInstance((AbstractType) valueClassInstance);
                Collection outputCollection = new ArrayList();
                outputCollection.addAll(listType.compose(buf));
                return marshalCollection(valueValidationClass, outputCollection, mapGenericClassses);
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (SecurityException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while retrieving field{} value via CQL, Caused by: .", clazz.getSimpleName(), e);
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Used to decompose and compose counter type data objects
     * 
     */
    private static class CounterTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return CounterColumnType.instance.decompose((Long) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return CounterColumnType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose date type data objects
     * 
     */
    private static class DateTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return DateType.instance.decompose((Date) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {

            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return DateType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose enum type data objects
     * 
     */
    private static class EnumTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            EnumAccessor accessor = new EnumAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            EnumAccessor accessor = new EnumAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose character type data objects
     * 
     */
    private static class CharacterTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            CharAccessor accessor = new CharAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            CharAccessor accessor = new CharAccessor();
            return accessor.fromString(clazz, new String((byte[]) value));
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decomposeCQL2(Object value, Class clazz)
        {
            CharAccessor charAccessor = new CharAccessor();
            return charAccessor.fromBytes(clazz, (byte[]) value);
        }
    }

    /**
     * Used to decompose and compose short type data objects
     * 
     */
    private static class ShortTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            ShortAccessor accessor = new ShortAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            IntegerAccessor accessor = new IntegerAccessor();
            int thriftColumnValue = accessor.fromBytes(short.class, (byte[]) value);
            ShortAccessor shortAccessor = new ShortAccessor();
            return shortAccessor.fromString(clazz, String.valueOf(thriftColumnValue));
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decomposeCQL2(Object value, Class clazz)
        {
            ShortAccessor shortAccessor = new ShortAccessor();
            return shortAccessor.fromBytes(clazz, (byte[]) value);
        }
    }

    /**
     * Used to decompose and compose big integer type data objects
     * 
     */
    private static class BigIntegerTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            return IntegerType.instance.decompose((BigInteger) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {

            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return IntegerType.instance.compose(buf);
        }
    }

    /**
     * Used to decompose and compose timestamp type data objects
     * 
     */
    private static class TimeStampTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            DateAccessor accessor = new DateAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            DateAccessor accessor = new DateAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose sql date type data objects
     * 
     */
    private static class SQLDateTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            SQLDateAccessor accessor = new SQLDateAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            SQLDateAccessor accessor = new SQLDateAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose sql time type data objects
     * 
     */
    private static class SQLTimeTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            SQLTimeAccessor accessor = new SQLTimeAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            SQLTimeAccessor accessor = new SQLTimeAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose sql timestamp type data objects
     * 
     */
    private static class SQLTimeStampTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            SQLTimestampAccessor accessor = new SQLTimestampAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            SQLTimestampAccessor accessor = new SQLTimestampAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose calendar type data objects
     * 
     */
    private static class CalendarTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            CalendarAccessor accessor = new CalendarAccessor();
            return accessor.toBytes(value);
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            CalendarAccessor accessor = new CalendarAccessor();
            return accessor.fromBytes(clazz, ((byte[]) value));
        }
    }

    /**
     * Used to decompose and compose bytes array type data objects
     * 
     */
    private static class BytesArrayTypeBuilder
    {
        /**
         * @param value
         * @param clazz
         * @return
         */
        private static byte[] compose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.hexToBytes((String) value).array();
        }

        /**
         * @param value
         * @param clazz
         * @return
         */
        private static Object decompose(Object value, Class clazz)
        {
            ByteBuffer buf = ByteBuffer.wrap((byte[]) value, 0, ((byte[]) value).length);
            return ByteBufferUtil.bytesToHex(buf);
        }
    }
}
