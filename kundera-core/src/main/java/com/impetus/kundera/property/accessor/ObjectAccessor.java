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
package com.impetus.kundera.property.accessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class ObjectAccessor.
 * 
 * @author animesh.kumar
 */
public class ObjectAccessor implements PropertyAccessor<Object>
{

    public static Logger log = LoggerFactory.getLogger(ObjectAccessor.class);

    /* @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[]) */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public final Object fromBytes(Class targetClass, byte[] bytes)
    {
        try
        {
            if (bytes == null)
            {
                return null;
            }
            if (targetClass != null && targetClass.equals(byte[].class))
            {
                return bytes;
            }
            ObjectInputStream ois;
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object o = ois.readObject();
            ois.close();
            return o;
        }
        catch (IOException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (ClassNotFoundException e)
        {
            throw new PropertyAccessException(e);
        }

    }

    /*
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    @Override
    public final byte[] toBytes(Object o)
    {

        try
        {
            if (o != null)
            {
                if (o instanceof byte[])
                {
                    return (byte[]) o;
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(o);
                oos.close();
                return baos.toByteArray();
            }
        }
        catch (IOException e)
        {
            throw new PropertyAccessException(e);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object)
     */
    @Override
    public final String toString(Object object)
    {
        return object != null ? object.toString() : null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String
     * )
     */
    @Override
    public Object fromString(Class targetClass, String s)
    {
        try
        {
            if (s == null)
            {
                return null;
            }
            Object o = (Object) s;
            return o;
        }
        catch (NumberFormatException e)
        {
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Object getCopy(Object object)
    {
        if (object == null)
            return null;

        if (object instanceof byte[])
        {
            byte[] byteArr = (byte[]) object;
            return byteArr.clone();
        }
        else if (object instanceof Cloneable)
        {
            Class<?> clazz = object.getClass();

            Object o = null;
            try
            {
                Method m = clazz.getMethod("clone");
                o = m.invoke(clazz);
            }
            catch (SecurityException e)
            {
                log.warn("Object of class " + object.getClass() + " can't be cloned, due to exception:"
                        + e.getMessage());
                return object;
            }
            catch (IllegalArgumentException e)
            {
                log.warn("Object of class " + object.getClass() + " can't be cloned, due to exception:"
                        + e.getMessage());
                return object;
            }
            catch (NoSuchMethodException e)
            {
                log.warn("Object of class " + object.getClass() + " can't be cloned, due to exception:"
                        + e.getMessage());
                return object;
            }
            catch (IllegalAccessException e)
            {
                log.warn("Object of class " + object.getClass() + " can't be cloned, due to exception:"
                        + e.getMessage());
                return object;
            }
            catch (InvocationTargetException e)
            {
                log.warn("Object of class " + object.getClass() + " can't be cloned, due to exception:"
                        + e.getMessage());
                return object;
            }
            return o;
        }
        else
        {
            return object;
        }
    }

    public Object getInstance(Class<?> clazz)
    {
        Object o = null;
        try
        {
            o = clazz.newInstance();
            return o;
        }
        catch (InstantiationException ie)
        {
            log.warn("Instantiation exception,caused by :" + ie.getMessage());
            return null;
        }
        catch (IllegalAccessException iae)
        {
            log.warn("Illegal access exception,caused by :" + iae.getMessage());
            return null;
        }
    }
}
