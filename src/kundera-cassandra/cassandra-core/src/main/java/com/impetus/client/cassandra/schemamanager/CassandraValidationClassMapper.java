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
package com.impetus.client.cassandra.schemamanager;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.serializers.*;

// TODO: Auto-generated Javadoc
/**
 * The Class CassandraValidationClassMapper holds the map of validation
 * class(e.g. wrapper for default_validation_class property) mapper.
 * 
 * @author Kuldeep.kumar
 */
public final class CassandraValidationClassMapper
{

    /** The Constant validationClassMapper. */
    private final static HashMap<Class<?>, Class<?>> validationClassMapper = new HashMap<Class<?>, Class<?>>();

    /** The Constant validationSerializerClassMapper. */
    private final static HashMap<Class<?>, TypeSerializer<?>> validationSerializerClassMapper = new HashMap<Class<?>, TypeSerializer<?>>();

    /** The Constant replication_strategies. */
    private final static List<String> replication_strategies = new ArrayList<String>();

    /** The validators and comparators. */
    private static List<String> validatorsAndComparators = new ArrayList<String>();
    static
    {
        // adding all possible strategies classes into list.
        replication_strategies.add(SimpleStrategy.class.getName());
        replication_strategies.add(NetworkTopologyStrategy.class.getName());

        // adding all possible validator and comparators into list.
        validatorsAndComparators.add(BytesType.class.getSimpleName());
        validatorsAndComparators.add(AsciiType.class.getSimpleName());
        validatorsAndComparators.add(UTF8Type.class.getSimpleName());
        validatorsAndComparators.add(Int32Type.class.getSimpleName());
        validatorsAndComparators.add(IntegerType.class.getSimpleName());
        validatorsAndComparators.add(LongType.class.getSimpleName());
        validatorsAndComparators.add(UUIDType.class.getSimpleName());
        validatorsAndComparators.add(DateType.class.getSimpleName());
        validatorsAndComparators.add(BooleanType.class.getSimpleName());
        validatorsAndComparators.add(FloatType.class.getSimpleName());
        validatorsAndComparators.add(DoubleType.class.getSimpleName());
        validatorsAndComparators.add(DecimalType.class.getSimpleName());
        validatorsAndComparators.add(CounterColumnType.class.getSimpleName());

        // putting possible combination into map.
        validationClassMapper.put(java.lang.String.class, UTF8Type.class);
        validationClassMapper.put(Character.class, UTF8Type.class);
        validationClassMapper.put(char.class, UTF8Type.class);

        validationClassMapper.put(java.sql.Time.class, DateType.class);
        validationClassMapper.put(java.lang.Integer.class, Int32Type.class);
        validationClassMapper.put(int.class, Int32Type.class);
        validationClassMapper.put(java.sql.Timestamp.class, DateType.class);
        validationClassMapper.put(Short.class, IntegerType.class);
        validationClassMapper.put(short.class, IntegerType.class);
        validationClassMapper.put(java.math.BigDecimal.class, DecimalType.class);
        validationClassMapper.put(java.sql.Date.class, DateType.class);
        validationClassMapper.put(java.util.Date.class, DateType.class);
        validationClassMapper.put(java.math.BigInteger.class, IntegerType.class);

        validationClassMapper.put(java.lang.Double.class, DoubleType.class);
        validationClassMapper.put(double.class, DoubleType.class);

        validationClassMapper.put(boolean.class, BooleanType.class);
        validationClassMapper.put(Boolean.class, BooleanType.class);

        validationClassMapper.put(java.lang.Long.class, LongType.class);
        validationClassMapper.put(long.class, LongType.class);

        validationClassMapper.put(Byte.class, BytesType.class);
        validationClassMapper.put(byte.class, BytesType.class);

        validationClassMapper.put(Float.class, FloatType.class);
        validationClassMapper.put(float.class, FloatType.class);

        validationClassMapper.put(UUID.class, UUIDType.class);

        validationClassMapper.put(Calendar.class, DateType.class);

        validationClassMapper.put(List.class, ListType.class);
        validationClassMapper.put(Set.class, SetType.class);
        validationClassMapper.put(Map.class, MapType.class);

        // validation serializer class mappers
        validationSerializerClassMapper.put(java.lang.String.class, UTF8Serializer.instance);
        validationSerializerClassMapper.put(Character.class, UTF8Serializer.instance);
        validationSerializerClassMapper.put(char.class, UTF8Serializer.instance);

        validationSerializerClassMapper.put(java.sql.Time.class, TimestampSerializer.instance);
        validationSerializerClassMapper.put(java.lang.Integer.class, Int32Serializer.instance);
        validationSerializerClassMapper.put(int.class, Int32Serializer.instance);
        validationSerializerClassMapper.put(java.sql.Timestamp.class, TimestampSerializer.instance);
        validationSerializerClassMapper.put(Short.class, IntegerSerializer.instance);
        validationSerializerClassMapper.put(short.class, IntegerSerializer.instance);
        validationSerializerClassMapper.put(java.math.BigDecimal.class, DecimalSerializer.instance);
        validationSerializerClassMapper.put(java.sql.Date.class, TimestampSerializer.instance);
        validationSerializerClassMapper.put(java.util.Date.class, TimestampSerializer.instance);
        validationSerializerClassMapper.put(java.math.BigInteger.class, IntegerSerializer.instance);

        validationSerializerClassMapper.put(java.lang.Double.class, DoubleSerializer.instance);
        validationSerializerClassMapper.put(double.class, DoubleSerializer.instance);

        validationSerializerClassMapper.put(boolean.class, BooleanSerializer.instance);
        validationSerializerClassMapper.put(Boolean.class, BooleanSerializer.instance);

        validationSerializerClassMapper.put(java.lang.Long.class, LongSerializer.instance);
        validationSerializerClassMapper.put(long.class, LongSerializer.instance);

        validationSerializerClassMapper.put(Byte.class, BytesSerializer.instance);
        validationSerializerClassMapper.put(byte.class, BytesSerializer.instance);

        validationSerializerClassMapper.put(Float.class, FloatSerializer.instance);
        validationSerializerClassMapper.put(float.class, FloatSerializer.instance);

        validationSerializerClassMapper.put(UUID.class, UUIDSerializer.instance);

        validationSerializerClassMapper.put(Calendar.class, TimestampSerializer.instance);

    }

    /**
     * Gets the validation class.
     * 
     * @param dataType
     *            the data type
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the validation class
     */
    public static String getValidationClass(Class<?> dataType, boolean isCql3Enabled)
    {
        return getValidationClassInstance(dataType, isCql3Enabled).getSimpleName();
    }

    /**
     * Gets the validation class instance.
     * 
     * @param dataType
     *            the data type
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the validation class instance
     */
    public static Class<?> getValidationClassInstance(Class<?> dataType, boolean isCql3Enabled)
    {
        resetMapperForCQL3(isCql3Enabled);
        Class<?> validation_class;
        validation_class = validationClassMapper.get(dataType);
        if (validation_class == null)
        {
            if (dataType.isEnum())
            {
                validation_class = UTF8Type.class;
            }
            else
            {
                validation_class = BytesType.class;
            }
        }
        resetMapperForThrift(isCql3Enabled);
        return validation_class;
    }

    /**
     * Gets the validation serializer class instance.
     * 
     * @param dataType
     *            the data type
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the validation serializer class instance
     */
    public static TypeSerializer<?> getValidationSerializerClassInstance(Class<?> dataType, boolean isCql3Enabled)
    {
        resetMapperForCQL3(isCql3Enabled);
        TypeSerializer<?> validation_class;
        validation_class = validationSerializerClassMapper.get(dataType);
        if (validation_class == null)
        {
            if (dataType.isEnum())
            {
                validation_class = UTF8Serializer.instance;
            }
            else
            {
                validation_class = BytesSerializer.instance;
            }
        }
        resetMapperForThrift(isCql3Enabled);
        return validation_class;
    }

    /**
     * Gets the value type name.
     * 
     * @param dataType
     *            the data type
     * @param genericClasses
     *            the generic classes
     * @param isCql3Enabled
     *            the is cql3 enabled
     * @return the value type name
     * @throws SyntaxException
     *             the syntax exception
     * @throws ConfigurationException
     *             the configuration exception
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws NoSuchFieldException
     *             the no such field exception
     * @throws SecurityException
     *             the security exception
     */
    public static String getValueTypeName(Class<?> dataType, List<Class<?>> genericClasses, boolean isCql3Enabled)
            throws SyntaxException, ConfigurationException, IllegalArgumentException, IllegalAccessException,
            NoSuchFieldException, SecurityException
    {
        String valueType;

        Class<?> validation_class = getValidationClassInstance(dataType, isCql3Enabled);

        valueType = validation_class.toString();
        if (validation_class.equals(ListType.class))
        {
            TypeParser parser = new TypeParser(getValidationClass(genericClasses.get(0), isCql3Enabled));
            valueType = ListType.getInstance(parser.parse(), true).toString();
        }
        else if (validation_class.equals(SetType.class))
        {
            TypeParser parser = new TypeParser(getValidationClass(genericClasses.get(0), isCql3Enabled));
            valueType = SetType.getInstance(parser.parse(), true).toString();
        }
        else if (validation_class.equals(MapType.class))
        {
            Class keyClass = CassandraValidationClassMapper.getValidationClassInstance(genericClasses.get(0), true);
            Class valueClass = CassandraValidationClassMapper.getValidationClassInstance(genericClasses.get(1), true);

            Object keyClassInstance = keyClass.getDeclaredField("instance").get(null);
            Object valueClassInstance = valueClass.getDeclaredField("instance").get(null);

            valueType = MapType.getInstance((AbstractType) keyClassInstance, (AbstractType) valueClassInstance, true)
                    .toString();
            // TypeParser keyParser = new
            // TypeParser(getValidationClass(genericClasses.get(0),
            // isCql3Enabled));
            // TypeParser valueParser = new
            // TypeParser(getValidationClass(genericClasses.get(1),
            // isCql3Enabled));
            // valueType = MapType.getInstance(keyParser,
            // valueParser).toString();
        }
        return valueType;
    }

    /**
     * Gets the replication strategies.
     * 
     * @return the replication strategies
     */
    public static List<String> getReplicationStrategies()
    {
        return replication_strategies;
    }

    /**
     * Gets the validators and comparators.
     * 
     * @return the validatorsAndComparators
     */
    public static List<String> getValidatorsAndComparators()
    {
        return validatorsAndComparators;
    }

    /**
     * Reset mapper for cq l3.
     * 
     * @param isCql3Enabled
     *            the is cql3 enabled
     */
    private static void resetMapperForCQL3(boolean isCql3Enabled)
    {
        if (isCql3Enabled)
        {
            validationClassMapper.put(Byte.class, Int32Type.class);
            validationClassMapper.put(byte.class, Int32Type.class);
            validationClassMapper.put(Short.class, Int32Type.class);
            validationClassMapper.put(short.class, Int32Type.class);
            validationClassMapper.put(java.sql.Time.class, TimestampType.class);
            validationClassMapper.put(java.sql.Date.class, TimestampType.class);
            validationClassMapper.put(java.util.Date.class, TimestampType.class);
            validationClassMapper.put(java.sql.Timestamp.class, TimestampType.class);

        }
    }

    /**
     * Reset mapper for thrift.
     * 
     * @param isCql3Enabled
     *            the is cql3 enabled
     */
    private static void resetMapperForThrift(boolean isCql3Enabled)
    {
        if (isCql3Enabled)
        {
            validationClassMapper.put(Byte.class, BytesType.class);
            validationClassMapper.put(byte.class, BytesType.class);
            validationClassMapper.put(Short.class, IntegerType.class);
            validationClassMapper.put(short.class, IntegerType.class);
            validationClassMapper.put(java.sql.Time.class, DateType.class);
            validationClassMapper.put(java.sql.Date.class, DateType.class);
            validationClassMapper.put(java.util.Date.class, DateType.class);
            validationClassMapper.put(java.sql.Timestamp.class, DateType.class);
        }
    }
}