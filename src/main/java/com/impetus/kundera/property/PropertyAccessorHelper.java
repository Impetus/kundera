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

import com.impetus.kundera.metadata.EntityMetadata;

/**
 * Helper class to access fields.
 * 
 * @author animesh.kumar
 */
public class PropertyAccessorHelper {

    /**
     * Sets a byte-array onto a field.
     * 
     * @param target
     *            the target
     * @param field
     *            the field
     * @param bytes
     *            the bytes
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void set(Object target, Field field, byte[] bytes) throws PropertyAccessException {

        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        Object value = accessor.fromBytes(bytes);
        set(target, field, value);
    }

    /**
     * Sets an object onto a field.
     * 
     * @param target
     *            the target
     * @param field
     *            the field
     * @param value
     *            the value
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void set(Object target, Field field, Object value) throws PropertyAccessException {

        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        try {
            field.set(target, value);
        } catch (IllegalArgumentException iarg) {
            throw new PropertyAccessException(iarg);
        } catch (IllegalAccessException iacc) {
            throw new PropertyAccessException(iacc);
        }
    }

    /**
     * Gets object from field.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the object
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static Object getObject(Object from, Field field) throws PropertyAccessException {

        if (!field.isAccessible()) {
            field.setAccessible(true);
        }

        try {
            return field.get(from);
        } catch (IllegalArgumentException iarg) {
            throw new PropertyAccessException(iarg);
        } catch (IllegalAccessException iacc) {
            throw new PropertyAccessException(iacc);
        }
    }

    /**
     * Gets the string.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the string
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static String getString(Object from, Field field) throws PropertyAccessException {

        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        return accessor.toString(getObject(from, field));

    }

    /**
     * Gets field value as byte-array.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the byte[]
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static byte[] get(Object from, Field field) throws PropertyAccessException {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        return accessor.toBytes(getObject(from, field));
    }

    /**
     * Gets id value of an entity class.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * 
     * @return the id
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static String getId(Object entity, EntityMetadata metadata) throws PropertyAccessException {
        return (String) getObject(entity, metadata.getIdProperty());
    }
}
