/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.property;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.property.accessor.IntegerAccessor;
import com.impetus.kundera.property.accessor.LongAccessor;
import com.impetus.kundera.property.accessor.ObjectAccessor;
import com.impetus.kundera.property.accessor.StringAccessor;

/**
 * The Class PropertyAccessorFactory.
 * 
 * @author animesh.kumar
 */
/**
 * @author animesh.kumar
 * 
 */
public class PropertyAccessorFactory {

    /** The map. */
    public static Map<Class<?>, PropertyAccessor<?>> map = new HashMap<Class<?>, PropertyAccessor<?>>();
    static {
        map.put(Integer.class, new IntegerAccessor());
        map.put(Long.class, new LongAccessor());
        map.put(String.class, new StringAccessor());
        map.put(Date.class, new DateAccessor());
        map.put(Object.class, new ObjectAccessor());
    }

    /** Making String Accessor easy to access. */
    public static final PropertyAccessor<String> STRING = new StringAccessor();

    /**
     * Instantiates a new property accessor factory.
     */
    private PropertyAccessorFactory() {
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
    public static PropertyAccessor getPropertyAccessor(Class<?> clazz) {
        PropertyAccessor<?> accessor = map.get(clazz);

        // allow fall-back to Object streamer.
        if (null == accessor) {
            accessor = map.get(Object.class);
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
    public static PropertyAccessor<?> getPropertyAccessor(Field property) {
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
    public static void add(Class<?> key, PropertyAccessor<?> value) {
        map.put(key, value);
    }

}
