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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;

/**
 * The Class CassandraValidationClassMapper holds the map of validation
 * class(e.g. wrapper for default_validation_class property) mapper.
 * 
 * @author Kuldeep.kumar
 */
public final class CassandraValidationClassMapper
{

    /** The Constant validationClassMapper. */
    private final static HashMap<Class<?>, String> validationClassMapper = new HashMap<Class<?>, String>();

    private final static List<String> replication_strategies = new ArrayList<String>();

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
        validationClassMapper.put(java.lang.String.class, UTF8Type.class.getSimpleName());
        validationClassMapper.put(char.class, UTF8Type.class.getSimpleName());

        validationClassMapper.put(java.sql.Time.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(java.lang.Integer.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(int.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(java.sql.Timestamp.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(short.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(java.math.BigDecimal.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(java.sql.Date.class, IntegerType.class.getSimpleName());
        validationClassMapper.put(java.util.Date.class, DateType.class.getSimpleName());
        validationClassMapper.put(java.math.BigInteger.class, IntegerType.class.getSimpleName());

        validationClassMapper.put(java.lang.Double.class, DoubleType.class.getSimpleName());
        validationClassMapper.put(double.class, DoubleType.class.getSimpleName());

        validationClassMapper.put(boolean.class, BooleanType.class.getSimpleName());

        validationClassMapper.put(java.lang.Long.class, LongType.class.getSimpleName());
        validationClassMapper.put(long.class, LongType.class.getSimpleName());

        validationClassMapper.put(byte.class, BytesType.class.getSimpleName());

        validationClassMapper.put(float.class, FloatType.class.getSimpleName());
        
        validationClassMapper.put(UUID.class, UUIDType.class.getSimpleName());
    }

    /**
     * Gets the validation class.
     * 
     * @param dataType
     *            the data type
     * @return the validation class
     */
    static String getValidationClass(Class<?> dataType)
    {
        String validation_class;
        validation_class = validationClassMapper.get(dataType);
        if (!(validation_class != null))
        {
            validation_class = BytesType.class.getSimpleName();
        }
        return validation_class;
    }

    public static List<String> getReplicationStrategies()
    {
        return replication_strategies;
    }

    /**
     * @return the validatorsAndComparators
     */
    public static List<String> getValidatorsAndComparators()
    {
        return validatorsAndComparators;
    }
}
