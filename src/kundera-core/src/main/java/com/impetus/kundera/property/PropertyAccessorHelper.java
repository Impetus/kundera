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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.impetus.kundera.client.EnhanceEntity;
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
    public static void set(Object target, Field field, byte[] bytes)
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        Object value = accessor.fromBytes(field.getType(), bytes);
        set(target, field, value);
    }

    /**
     * Sets a byte-array onto a field.
     * 
     * @param target
     *            the target
     * @param field
     *            the field
     * @param fieldVal
     *            the field value
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void set(Object target, Field field, String fieldVal)
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        Object value = accessor.fromString(target.getClass(), fieldVal);
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
    public static void set(Object target, Field field, Object value)
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
    public static Object getObject(Object from, Field field)
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
     * Retutrns copy of object
     * 
     * @param from
     * @param field
     * @return
     */
    public static Object getObjectCopy(Object from, Field field)
    {

        if (!field.isAccessible())
        {
            field.setAccessible(true);
        }

        try
        {
            PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
            return accessor.getCopy(field.get(from));
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
    public static String getString(Object from, Field field)
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        Object object = getObject(from, field);
        return object != null ? accessor.toString(object) : null;

    }

    /*
    *//**
     * Invokes corresponding accessor and returns string value for that
     * object.
     * 
     * @param obj
     *            object.
     * 
     * @return string value for input object.
     */
    /*
     * public static String getString(Object obj) { PropertyAccessor<?> accessor
     * = PropertyAccessorFactory.getPropertyAccessor(obj.getClass()); return
     * accessor.toString(obj);
     * 
     * }
     */
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
    public static byte[] get(Object from, Field field)
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
    public static Object getId(Object entity, EntityMetadata metadata)
    {

        // If an Entity has been wrapped in a Proxy, we can call the Proxy
        // classes' getId() method
        if (entity instanceof EnhanceEntity)
        {
            return ((EnhanceEntity) entity).getEntityId();
        }

        // Otherwise, as Kundera currently supports only field access, access
        // the underlying Entity's id field

        return getObject(entity, (Field) metadata.getIdAttribute().getJavaMember());
    }

    /**
     * Sets Primary Key (Row key) into entity field that was annotated with @Id.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * @param rowKey
     *            the row key
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void setId(Object entity, EntityMetadata metadata, Object rowKey)
    {
        try
        {
            Field idField = (Field) metadata.getIdAttribute().getJavaMember();
            set(entity, idField, rowKey);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
    }

    /**
     * Sets Primary Key (Row key) into entity field that was annotated with @Id.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * @param rowKey
     *            the row key
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void setId(Object entity, EntityMetadata metadata, byte[] rowKey)
    {
        try
        {
            Field idField = (Field) metadata.getIdAttribute().getJavaMember();

            // PropertyAccessor<?> accessor =
            // PropertyAccessorFactory.getPropertyAccessor(idField);
            // Object obj = accessor.fromBytes(idField.getClass(), rowKey);
            //
            // metadata.getWriteIdentifierMethod().invoke(entity, obj);
            //
            set(entity, idField, rowKey);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
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
     * @throws PropertyAccessException
     *             the property access exception
     */
    @SuppressWarnings("null")
    // TODO: Too much code, improve this, possibly by breaking it
    public static final Object getObject(Object obj, String fieldName)
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
                if (embededObject == null)
                {
                    Class embeddedObjectClass = embeddedField.getType();
                    if (Collection.class.isAssignableFrom(embeddedObjectClass))
                    {
                        if (embeddedObjectClass.equals(List.class))
                        {
                            return new ArrayList();
                        }
                        else if (embeddedObjectClass.equals(Set.class))
                        {
                            return new HashSet();
                        }
                    }
                    else
                    {
                        embededObject = embeddedField.getType().newInstance();
                        embeddedField.set(obj, embededObject);
                    }

                }
                return embededObject;
            }
            else
            {
                throw new PropertyAccessException("Embedded object not found: " + fieldName);
            }

        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e);
        }
    }

    /**
     * Retrieves Generic class from a collection field that has only one
     * argument.
     * 
     * @param collectionField
     *            the collection field
     * @return the generic class
     */
    public static Class<?> getGenericClass(Field collectionField)
    {
        Class<?> genericClass = null;
        if (collectionField == null)
        {
            return genericClass;
        }
        if (isCollection(collectionField.getType()))
        {
            Type[] parameters = ReflectUtils.getTypeArguments(collectionField);
            if (parameters != null)
            {
                if (parameters.length == 1)
                {
                    genericClass = toClass(parameters[0]);
                }
                else
                {
                    throw new PropertyAccessException(
                            "Can't determine generic class from a field that has more than one parameters.");
                }
            }
        }
        return genericClass != null ? genericClass : collectionField.getType();
    }

    /**
     * Retrieves Generic class from a collection field that has only one
     * argument.
     * 
     * @param collectionField
     *            the collection field
     * @return the generic class
     */
    public static List<Class<?>> getGenericClasses(Field collectionField)
    {
        List<Class<?>> genericClasses = new ArrayList<Class<?>>();
        if (collectionField == null)
        {
            return genericClasses;
        }
        Type[] parameters = ReflectUtils.getTypeArguments(collectionField);
        if (parameters != null)
        {

            for (Type parameter : parameters)
            {
                // workaround for jdk1.6 issue.
                genericClasses.add(toClass(parameter));

            }

        }

        return genericClasses;
    }

    /**
     * Gets the declared fields.
     * 
     * @param relationalField
     *            the relational field
     * @return the declared fields
     */
    public static Field[] getDeclaredFields(Field relationalField)
    {
        Field[] fields;
        if (isCollection(relationalField.getType()))
        {
            fields = PropertyAccessorHelper.getGenericClass(relationalField).getDeclaredFields();
        }
        else
        {
            fields = relationalField.getType().getDeclaredFields();
        }
        return fields;
    }

    /**
     * Checks if is collection.
     * 
     * @param clazz
     *            the clazz
     * @return true, if is collection
     */
    public static final boolean isCollection(Class<?> clazz)
    {
        return Collection.class.isAssignableFrom(clazz);

    }

    public static final Object getObject(Class<?> clazz)
    {
        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(clazz);
        return accessor.getInstance(clazz);
    }

    public static final byte[] toBytes(Object o, Field f)
    {
        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(f);
        return accessor.toBytes(o);
    }

    public static final byte[] toBytes(Object o, Class c)
    {
        PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(c);
        return accessor.toBytes(o);
    }

    public static Object fromSourceToTargetClass(Class<?> targetClass, Class<?> sourceClass, Object o)
    {
        if (!targetClass.equals(sourceClass))
        {
            PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(sourceClass);
            String s = accessor.toString(o);
            accessor = PropertyAccessorFactory.getPropertyAccessor(targetClass);
            return accessor.fromString(targetClass, s);
        }
        return o;
    }

    public static Object fromDate(Class<?> targetClass, Class<?> sourceClass, Object o)
    {
        if (!targetClass.equals(sourceClass))
        {
            PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(sourceClass);
            byte[] b = accessor.toBytes(o);
            accessor = PropertyAccessorFactory.getPropertyAccessor(targetClass);
            return accessor.fromBytes(targetClass, b);
        }
        return o;
    }

    public static byte[] getBytes(Object o)
    {
        return PropertyAccessorFactory.getPropertyAccessor(o.getClass()).toBytes(o);
    }

    public static String getString(Object o)
    {
        return o != null ? PropertyAccessorFactory.getPropertyAccessor(o.getClass()).toString(o) : null;
    }

    public static Object getObject(Class clazz, byte[] b)
    {
        return PropertyAccessorFactory.getPropertyAccessor(clazz).fromBytes(clazz, b);
    }

    public static final Collection getCollectionInstance(Field collectionField)
    {
        if (collectionField != null)
        {
            if (collectionField.getType().isAssignableFrom(List.class))
            {
                return new ArrayList();
            }
            else if (collectionField.getType().isAssignableFrom(Set.class))
            {
                return new HashSet();
            }
        }
        return null;
    }

    /**
     * Borrowed from java.lang.class
     * @param o
     * @return
     */
    
    private static Class<?> toClass(Type o)
    {
        if (o instanceof GenericArrayType)
        {
            Class clazz = Array.newInstance(toClass(((GenericArrayType) o).getGenericComponentType()), 0).getClass();
            return clazz;
        }
        return (Class<?>) o;
    }
}
