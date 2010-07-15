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
package com.impetus.kundera.property.accessor;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * Base implementation of PropertyAccessor.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public abstract class BasePropertyAccessor<T> implements PropertyAccessor<T> {

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#get(java.lang.Object,
     * java.lang.reflect.Field, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Map<String, byte[]> readAsByteArray(Object target, Field property, String alias) throws PropertyAccessException {
        // make sure that the property is accessible
        if (!property.isAccessible()) {
            property.setAccessible(true);
        }

        try {
            T value = (T) property.get(target);

            if (null == value)
                throw new PropertyAccessException(target.getClass().getName() + "." + property.getName() + " is null.");

            Map<String, byte[]> map = new HashMap<String, byte[]>();
            map.put(alias, toBytes(value));
            return map;
        } catch (IllegalArgumentException iarg) {
            throw new PropertyAccessException(iarg.getMessage());
        } catch (IllegalAccessException iacc) {
            throw new PropertyAccessException(iacc.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#readAsObject(java.lang.
     * Object, java.lang.reflect.Field, java.lang.String)
     */
    @Override
    public final Map<String, T> readAsObject(Object target, Field property, String alias) throws PropertyAccessException {
        Map<String, T> map = new HashMap<String, T>();
        for (Map.Entry<String, byte[]> entry : readAsByteArray(target, property, alias).entrySet()) {
            map.put(entry.getKey(), fromBytes(entry.getValue()));
        }
        return map;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#set(java.lang.Object,
     * java.lang.reflect.Field, byte[], java.lang.String)
     */
    @Override
    public final void set(Object target, Field property, byte[] bytes, String alias) throws PropertyAccessException {

        // make sure that the property is accessible
        if (!property.isAccessible()) {
            property.setAccessible(true);
        }

        try {
            property.set(target, fromBytes(bytes));
        } catch (IllegalArgumentException iarg) {
            throw new PropertyAccessException(iarg);
        } catch (IllegalAccessException iacc) {
            throw new PropertyAccessException(iacc);
        }
    }

}
