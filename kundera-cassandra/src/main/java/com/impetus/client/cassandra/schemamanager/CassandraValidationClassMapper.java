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

/**
 * The Class CassandraValidationClassMapper holds the map of validation
 * class(e.g. wrapper for default_validation_class property) mapper.
 * 
 * @author Kuldeep.kumar
 */
final class CassandraValidationClassMapper
{

    /** The Constant validationClassMapper. */
    private final static HashMap<Class, String> validationClassMapper = new HashMap<Class, String>();

    private final static List<String> replication_startegies = new ArrayList<String>();

    static
    {

        replication_startegies.add("org.apache.cassandra.locator.SimpleStrategy");
        replication_startegies.add("org.apache.cassandra.locator.NetworkTopologyStrategy");

        validationClassMapper.put(java.lang.String.class, "UTF8Type");
        validationClassMapper.put(char.class, "UTF8Type");

        validationClassMapper.put(java.sql.Time.class, "IntegerType");
        validationClassMapper.put(java.lang.Integer.class, "IntegerType");
        validationClassMapper.put(int.class, "IntegerType");
        validationClassMapper.put(java.sql.Timestamp.class, "IntegerType");
        validationClassMapper.put(short.class, "IntegerType");
        validationClassMapper.put(java.math.BigDecimal.class, "IntegerType");
        validationClassMapper.put(java.sql.Date.class, "IntegerType");
        validationClassMapper.put(java.util.Date.class, "IntegerType");
        validationClassMapper.put(java.math.BigInteger.class, "IntegerType");

        validationClassMapper.put(java.lang.Double.class, "DoubleType");
        validationClassMapper.put(double.class, "DoubleType");

        validationClassMapper.put(boolean.class, "BooleanType");

        validationClassMapper.put(java.lang.Long.class, "LongType");
        validationClassMapper.put(long.class, "LongType");

        validationClassMapper.put(byte.class, "BytesType");

        validationClassMapper.put(float.class, "FloatType");
    }

    /**
     * Gets the validation class.
     * 
     * @param dataType
     *            the data type
     * @return the validation class
     */
    static String getValidationClass(Class dataType)
    {
        String validation_class;
        validation_class = validationClassMapper.get(dataType);
        if (!(validation_class != null))
        {
            validation_class = "BytesType";
        }
        return validation_class;
    }

    static List<String> getReplicationStrategies()
    {
        return replication_startegies;
    }
}
