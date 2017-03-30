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
package com.impetus.kundera.property;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.property.accessor.BigDecimalAccessor;
import com.impetus.kundera.property.accessor.BigIntegerAccessor;
import com.impetus.kundera.property.accessor.BooleanAccessor;
import com.impetus.kundera.property.accessor.ByteAccessor;
import com.impetus.kundera.property.accessor.CalendarAccessor;
import com.impetus.kundera.property.accessor.CharAccessor;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.property.accessor.DoubleAccessor;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.impetus.kundera.property.accessor.FloatAccessor;
import com.impetus.kundera.property.accessor.IntegerAccessor;
import com.impetus.kundera.property.accessor.LongAccessor;
import com.impetus.kundera.property.accessor.ObjectAccessor;
import com.impetus.kundera.property.accessor.PointAccessor;
import com.impetus.kundera.property.accessor.SQLDateAccessor;
import com.impetus.kundera.property.accessor.SQLTimeAccessor;
import com.impetus.kundera.property.accessor.SQLTimestampAccessor;
import com.impetus.kundera.property.accessor.ShortAccessor;
import com.impetus.kundera.property.accessor.StringAccessor;
import com.impetus.kundera.property.accessor.UUIDAccessor;

/**
 * The Class PropertyAccessorFactory.
 * 
 * @author animesh.kumar
 */

public class PropertyAccessorFactory
{

    /** The map. */
    public static Map<Class<?>, PropertyAccessor<?>> map = new HashMap<Class<?>, PropertyAccessor<?>>();

    static
    {
        // Premitive Type accessors
        map.put(boolean.class, new BooleanAccessor());
        map.put(byte.class, new ByteAccessor());
        map.put(short.class, new ShortAccessor());
        map.put(char.class, new CharAccessor());
        map.put(int.class, new IntegerAccessor());
        map.put(long.class, new LongAccessor());
        map.put(float.class, new FloatAccessor());
        map.put(double.class, new DoubleAccessor());

        // Wrapper Object accessors
        map.put(Boolean.class, new BooleanAccessor());
        map.put(Byte.class, new ByteAccessor());
        map.put(Short.class, new ShortAccessor());
        map.put(Character.class, new CharAccessor());
        map.put(Integer.class, new IntegerAccessor());
        map.put(Long.class, new LongAccessor());
        map.put(Float.class, new FloatAccessor());
        map.put(Double.class, new DoubleAccessor());

        // Date/ Time type accessors
        map.put(Date.class, new DateAccessor());
        map.put(java.sql.Date.class, new SQLDateAccessor());
        map.put(Time.class, new SQLTimeAccessor());
        map.put(Timestamp.class, new SQLTimestampAccessor());
        map.put(Calendar.class, new CalendarAccessor());
        map.put(GregorianCalendar.class, new CalendarAccessor());

        // Accessors for Math classes
        map.put(BigInteger.class, new BigIntegerAccessor());
        map.put(BigDecimal.class, new BigDecimalAccessor());

        // String class Accessor
        map.put(String.class, new StringAccessor());

        // Accessor for the generic object
        map.put(Object.class, new ObjectAccessor());

        // Accessor for Enum types
        map.put(Enum.class, new EnumAccessor());

        map.put(UUID.class, new UUIDAccessor());

        // Accessor for Geolocation classes
        map.put(Point.class, new PointAccessor());
    }

    /** Making String Accessor easy to access. */
    public static final PropertyAccessor<String> STRING = new StringAccessor();

    /**
     * Instantiates a new property accessor factory.
     */
    private PropertyAccessorFactory()
    {
    }

    /**
     * Gets the property accessor.
     * 
     * @param clazz
     *            the clazz
     * 
     * @return the property accessor
     */
    @SuppressWarnings("unchecked")
    public static PropertyAccessor getPropertyAccessor(Class<?> clazz)
    {
        PropertyAccessor<?> accessor;
        if (clazz.isEnum())
        {
            accessor = new EnumAccessor();
        }
        else
        {
            accessor = map.get(clazz);
        }

        // allow fall-back to Object streamer.
        if (null == accessor)
        {
            if (Enum.class.isAssignableFrom(clazz))
            {
                accessor = map.get(Enum.class);
            }
            else
            {
                accessor = map.get(Object.class);
            }

        }
        return accessor;
    }

    /**
     * Gets the property accessor.
     * 
     * @param property
     *            the property
     * 
     * @return the property accessor
     */
    public static PropertyAccessor<?> getPropertyAccessor(Field property)
    {
        return getPropertyAccessor(property.getType());
    }

    /**
     * Adds the.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public static void add(Class<?> key, PropertyAccessor<?> value)
    {
        map.put(key, value);
    }

}
