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
package com.impetus.kundera.datatypes.datagenerator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The Class DataGeneratorFactory.
 * 
 * @author kuldeep.kumar
 */

public class DataGeneratorFactory
{
    /** The map. */
    private static Map<Class<?>, DataGenerator<?>> map = new HashMap<Class<?>, DataGenerator<?>>();

    static
    {
        // Premitive Type generators
        map.put(boolean.class, new BooleanDataGenerator());
        map.put(byte.class, new ByteDataGenerator());
        map.put(short.class, new ShortDataGenerator());
        map.put(char.class, new CharDataGenerator());
        map.put(int.class, new IntegerDataGenerator());
        map.put(long.class, new LongDataGenerator());
        map.put(float.class, new FloatDataGenerator());
        map.put(double.class, new DoubleDataGenerator());

        // Wrapper Object generators
        map.put(Boolean.class, new BooleanDataGenerator());
        map.put(Byte.class, new ByteDataGenerator());
        map.put(Short.class, new ShortDataGenerator());
        map.put(Character.class, new CharDataGenerator());
        map.put(Integer.class, new IntegerDataGenerator());
        map.put(Long.class, new LongDataGenerator());
        map.put(Float.class, new FloatDataGenerator());
        map.put(Double.class, new DoubleDataGenerator());

        // Date/ Time type generators
        map.put(Date.class, new DateDataGenerator());
        map.put(java.sql.Date.class, new SqlDateDataGenerator());
        map.put(Time.class, new SqlTimeDataGenerator());
        map.put(Timestamp.class, new SqlTimestampDataGenerator());
        map.put(Calendar.class, new CalendarDataGenerator());

        // Generators for Math classes
        map.put(BigInteger.class, new BigIntegerDataGenerator());
        map.put(BigDecimal.class, new BigDecimalDataGenerator());

        // String class generators
        map.put(String.class, new StringDataGenerator());

        map.put(UUID.class, new UUIDDataGenerator());
    }

    // /**
    // * Instantiates a new data generator factory.
    // */
    // private DataGeneratorFactory()
    // {
    //
    // }

    public DataGenerator<?> getDataGenerator(Class<?> clazz)
    {
        return map.get(clazz);
    }
}
