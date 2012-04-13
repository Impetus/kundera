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

import java.util.HashMap;

/**
 * The Class CassandraValidationClassMapper holds the map of validation
 * class(e.g. wrapper for default_validation_class property) mapper.
 * 
 * @author Kuldeep.kumar
 */
final class CassandraValidationClassMapper
{

    /** The Constant validationClassMapper. */
    final static HashMap<String, String> validationClassMapper = new HashMap<String, String>();

    static
    {

        validationClassMapper.put("java.lang.String", "UTF8Type");
        validationClassMapper.put("char", "UTF8Type");

        validationClassMapper.put("java.sql.Time", "IntegerType");
        validationClassMapper.put("java.lang.Integer", "IntegerType");
        validationClassMapper.put("int", "IntegerType");
        validationClassMapper.put("java.sql.Timestamp", "IntegerType");
        validationClassMapper.put("short", "IntegerType");
        validationClassMapper.put("java.math.BigDecimal", "IntegerType");
        validationClassMapper.put("java.sql.Date", "IntegerType");
        validationClassMapper.put("java.util.Date", "IntegerType");
        validationClassMapper.put("java.math.BigInteger", "IntegerType");

        validationClassMapper.put("java.lang.Double", "DoubleType");
        validationClassMapper.put("double", "DoubleType");

        validationClassMapper.put("boolean", "BooleanType");

        validationClassMapper.put("java.lang.Long", "LongType");
        validationClassMapper.put("long", "LongType");

        validationClassMapper.put("byte", "BytesType");

        validationClassMapper.put("float", "FloatType");
    }

    /**
     * Gets the validation class.
     * 
     * @param dataType
     *            the data type
     * @return the validation class
     */
    static String getValidationClass(String dataType)
    {
        String validation_class;
        validation_class = validationClassMapper.get(dataType);
        if (!(validation_class != null))
        {
            validation_class = "BytesType";
        }
        return validation_class;
    }
}
