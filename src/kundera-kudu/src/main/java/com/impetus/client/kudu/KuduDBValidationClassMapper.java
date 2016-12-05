/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu;

import java.util.Calendar;
import java.util.HashMap;

import org.apache.kudu.Type;

/**
 * The Class KuduDBValidationClassMapper.
 * 
 * @author karthikp.manchala
 */
public class KuduDBValidationClassMapper
{

    /** The Constant validationClassMapper. */
    private final static HashMap<Class<?>, Type> validationClassMapper = new HashMap<Class<?>, Type>();

    static
    {
        validationClassMapper.put(java.lang.String.class, Type.STRING);
        validationClassMapper.put(Character.class, Type.STRING);
        validationClassMapper.put(char.class, Type.STRING);

        validationClassMapper.put(java.sql.Time.class, Type.UNIXTIME_MICROS);
        validationClassMapper.put(java.lang.Integer.class, Type.INT32);
        validationClassMapper.put(int.class, Type.INT32);
        validationClassMapper.put(java.sql.Timestamp.class, Type.UNIXTIME_MICROS);
        validationClassMapper.put(Short.class, Type.INT16);
        validationClassMapper.put(short.class, Type.INT16);
        validationClassMapper.put(java.sql.Date.class, Type.UNIXTIME_MICROS);
        validationClassMapper.put(java.util.Date.class, Type.UNIXTIME_MICROS);
        validationClassMapper.put(java.math.BigInteger.class, Type.INT64);
        validationClassMapper.put(java.math.BigDecimal.class, Type.INT64);

        validationClassMapper.put(java.lang.Double.class, Type.DOUBLE);
        validationClassMapper.put(double.class, Type.DOUBLE);

        validationClassMapper.put(boolean.class, Type.BOOL);
        validationClassMapper.put(Boolean.class, Type.BOOL);

        validationClassMapper.put(java.lang.Long.class, Type.INT64);
        validationClassMapper.put(long.class, Type.INT64);

        validationClassMapper.put(Byte.class, Type.INT8);
        validationClassMapper.put(byte.class, Type.INT8);

        validationClassMapper.put(Float.class, Type.FLOAT);
        validationClassMapper.put(float.class, Type.FLOAT);

        validationClassMapper.put(Byte[].class, Type.BINARY);
        validationClassMapper.put(byte[].class, Type.BINARY);

        validationClassMapper.put(Calendar.class, Type.UNIXTIME_MICROS);
    }

    /**
     * Gets the valid type for class.
     * 
     * @param clazz
     *            the clazz
     * @return the valid type for class
     */
    public static Type getValidTypeForClass(Class clazz)
    {
        return validationClassMapper.get(clazz);
    }
}
