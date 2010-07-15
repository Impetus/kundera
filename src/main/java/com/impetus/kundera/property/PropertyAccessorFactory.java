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
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import com.impetus.kundera.property.accessor.CollectionAccessor;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.property.accessor.IntegerAccessor;
import com.impetus.kundera.property.accessor.LongAccessor;
import com.impetus.kundera.property.accessor.MapAccessor;
import com.impetus.kundera.property.accessor.ObjectAccessor;
import com.impetus.kundera.property.accessor.StringAccessor;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class PropertyAccessorFactory.
 * 
 * @author animesh.kumar
 */
public class PropertyAccessorFactory {

    /** The Constant INTEGER. */
    public static final PropertyAccessor<Integer> INTEGER = new IntegerAccessor();

    /** The Constant LONG. */
    public static final PropertyAccessor<Long> LONG = new LongAccessor();

    /** The Constant STRING. */
    public static final PropertyAccessor<String> STRING = new StringAccessor();

    /** The Constant DATE. */
    public static final PropertyAccessor<Date> DATE = new DateAccessor();

    /** The Constant COLLECTION. */
    public static final PropertyAccessor<Object> COLLECTION = new CollectionAccessor();

    /** The Constant MAP. */
    public static final PropertyAccessor<Object> MAP = new MapAccessor();

    /** The Constant OBJECT. */
    public static final PropertyAccessor<Object> OBJECT = new ObjectAccessor();

    /**
     * Gets the property accessor for string.
     * 
     * @return the property accessor for string
     */
    public static PropertyAccessor<String> getPropertyAccessorForString() {
        return STRING;
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

        if (clazz.equals(Integer.class)) {
            return INTEGER;
        } else if (clazz.equals(String.class)) {
            return STRING;
        } else if (clazz.equals(Long.class)) {
            return LONG;
        } else if (clazz.equals(Date.class)) {
            return DATE;
        } else if (clazz.equals(Collection.class)) {
            return COLLECTION;
        } else if (clazz.equals(Map.class)) {
            return MAP;
        } else if (ReflectUtils.hasInterface(Collection.class, clazz)) {
            return COLLECTION;
        } else if (ReflectUtils.hasInterface(Map.class, clazz)) {
            return MAP;
        }

        return OBJECT;
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
     * Helper method to set string property.
     * 
     * @param target
     *            the target
     * @param property
     *            the property
     * @param key
     *            the key
     * 
     * @throws Exception
     *             * @throws PropertyAccessException the property access
     *             exception
     */
    public static void setStringProperty(Object target, Field property, String key) throws PropertyAccessException {
        setProperty(target, property, STRING.toBytes(key), property.getName(), null);
    }

    /**
     * Helper method to get string property.
     * 
     * @param target
     *            the target
     * @param property
     *            the property
     * 
     * @return the string property
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static String getStringProperty(Object target, Field property) throws PropertyAccessException {
        byte[] idValue = PropertyAccessorFactory.getPropertyAccessor(property).readAsByteArray(target, property, "some-dummy-key-here###").get("some-dummy-key-here###");
        return STRING.fromBytes(idValue);
    }

    /**
     * Helper method to set property value.
     * 
     * @param target
     *            the target
     * @param property
     *            the property
     * @param value
     *            the value
     * @param propertyName
     *            the property name
     * @param alias
     *            the alias
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void setProperty(Object target, Field property, byte[] value, String propertyName, String alias) throws PropertyAccessException {
        // set value of the field in the bean
        PropertyAccessorFactory.getPropertyAccessor(property).set(target, property, value, alias);
    }

}
