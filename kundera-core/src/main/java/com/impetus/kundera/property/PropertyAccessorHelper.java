/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.PersistenceException;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Helper class to access fields.
 *
 * @author animesh.kumar
 */
public class PropertyAccessorHelper
{

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
    public static void set(Object target, Field field, byte[] bytes) throws PropertyAccessException
    {

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
    public static void set(Object target, Field field, Object value) throws PropertyAccessException
    {

        if (!field.isAccessible())
        {
            field.setAccessible(true);
        }
        try
        {
            field.set(target, value);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
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
    public static Object getObject(Object from, Field field) throws PropertyAccessException
    {

        if (!field.isAccessible())
        {
            field.setAccessible(true);
        }

        try
        {
            return field.get(from);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
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
    public static String getString(Object from, Field field) throws PropertyAccessException
    {

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
    public static byte[] get(Object from, Field field) throws PropertyAccessException
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        return accessor.toBytes(getObject(from, field));
    }

    /**
     * Get identifier of an entity object by invoking getXXX() method.
     *
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
    public static String getId(Object entity, EntityMetadata metadata) throws PropertyAccessException
    {
        try
        {
            // Always read from method. that way, even LazyInitialized
            // Proxy classes can be worked upon.
            return (String) metadata.getReadIdentifierMethod().invoke(entity, new Object[] {});
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
            throw new PropertyAccessException(iacc);
        }
        catch (InvocationTargetException ite)
        {
            throw new PropertyAccessException(ite);
        }
    }

    /**
     * Gets the embedded object.
     *
     * @param obj
     *            the obj
     * @param fieldName
     *            the field name
     * @return the embedded object
     */
    @SuppressWarnings("null")
    //TODO: Too much code, improve this, possibly by breaking it
    public static final Object getObject(Object obj, String fieldName) throws PropertyAccessException
    {
        Field embeddedField;
        try
        {
            embeddedField = obj.getClass().getDeclaredField(fieldName);
            if (embeddedField != null)
            {
                if (!embeddedField.isAccessible())
                {
                    embeddedField.setAccessible(true);
                }
                Object embededObject = embeddedField.get(obj);
                if(embededObject == null)
                {
                    Class embeddedObjectClass = embeddedField.getType();
                    if(Collection.class.isAssignableFrom(embeddedObjectClass)) {
                       if(embeddedObjectClass.equals(List.class)) {
                           return new ArrayList();
                       } else if(embeddedObjectClass.equals(Set.class)) {
                           return new HashSet();
                       }
                    } else {
                        embededObject = embeddedField.getType().newInstance();
                        embeddedField.set(obj, embededObject);
                    }          
                    
                }
                return embededObject;
            }
            else
            {
                throw new RuntimeException("Embedded object not found: " + fieldName);
            }

        }
        catch (SecurityException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (NoSuchFieldException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (IllegalArgumentException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (InstantiationException e)
        {
            throw new PropertyAccessException(e);
        }
    }

    /**
     * Retrieves Generic class from a collection field
     *
     * @param collectionField
     * @return
     */
    public static Class<?> getGenericClass(Field collectionField)
    {
        Class<?> genericClass = null;
        Type[] parameters = ReflectUtils.getTypeArguments(collectionField);
        if (parameters != null)
        {
            if (parameters.length == 1)
            {
                genericClass = (Class<?>) parameters[0];
            }
            else
            {
                throw new PersistenceException("Can't determine generic class from a field that has two parameters.");
            }
        }
        return genericClass;
    }
}
