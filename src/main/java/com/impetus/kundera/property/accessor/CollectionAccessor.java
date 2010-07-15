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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class CollectionAccessor.
 * 
 * @author animesh.kumar
 */
public class CollectionAccessor implements PropertyAccessor<Object> {

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#get(java.lang.Object,
     * java.lang.reflect.Field, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, byte[]> readAsByteArray(Object target, Field property, String alias) throws PropertyAccessException {

        if (!property.isAccessible()) {
            property.setAccessible(true);
        }

        Map<String, byte[]> map = new HashMap<String, byte[]>();

        Class<?> propertyArgumentType = getArgumentType(property);

        try {
            Object value = property.get(target);
            Iterable<?> itrble = (Iterable<?>) value;
            Iterator itr = itrble.iterator();
            int counter = 0;
            while (itr.hasNext()) {
                String key = alias + Constants.SEPARATOR + counter++;
                Object next = itr.next();

                byte[] bytes = PropertyAccessorFactory.getPropertyAccessor(propertyArgumentType).toBytes(next);
                map.put(key, bytes);
            }
        } catch (IllegalArgumentException e1) {
            throw new PropertyAccessException(e1);
        } catch (IllegalAccessException e1) {
            throw new PropertyAccessException(e1);
        }

        return map;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#readAsObject(java.lang.
     * Object, java.lang.reflect.Field, java.lang.String)
     */
    @Override
    public Map<String, Object> readAsObject(Object target, Field property, String alias) throws PropertyAccessException {
        Map<String, Object> map = new HashMap<String, Object>();
        for (Map.Entry<String, byte[]> entry : readAsByteArray(target, property, alias).entrySet()) {
            map.put(entry.getKey(), PropertyAccessorFactory.getPropertyAccessor(getArgumentType(property)).fromBytes(entry.getValue()));
        }
        return map;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#set(java.lang.Object,
     * java.lang.reflect.Field, byte[], java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void set(Object target, Field property, byte[] bytes, String alias) throws PropertyAccessException {

        if (!property.isAccessible()) {
            property.setAccessible(true);
        }

        Class<?> propertyArgumentType = getArgumentType(property);

        try {
            Collection coll = (Collection) property.get(target);
            coll.add(PropertyAccessorFactory.getPropertyAccessor(propertyArgumentType).fromBytes(bytes));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new PropertyAccessException(e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new PropertyAccessException(e);
        }
    }

    /**
     * Gets the argument type.
     * 
     * @param property
     *            the property
     * 
     * @return the argument type
     */
    @SuppressWarnings("unchecked")
    public Class getArgumentType(Field property) {
        try {
            return (Class) ReflectUtils.getTypeArguments(property)[0];
        } catch (Exception e) {
            return Object.class;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#decode(byte[])
     */
    @Override
    public Object fromBytes(byte[] b) throws PropertyAccessException {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#encode(java.lang.Object)
     */
    @Override
    public byte[] toBytes(Object value) throws PropertyAccessException {
        return null;
    }

}
