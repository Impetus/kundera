/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.utils;

import java.math.BigDecimal;

import com.impetus.client.hbase.query.SingleColumnFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * The Class HBaseUtils.
 */
public final class HBaseUtils
{

    /**
     * Gets the bytes.
     * 
     * @param value
     *            the value
     * @param clazz
     *            the clazz
     * @return the bytes
     */
    public static byte[] getBytes(Object value, Class<?> clazz)
    {
        if (/* isId || */clazz.isAssignableFrom(String.class))
        {
            return Bytes.toBytes(value.toString());
        }
        else if (clazz.equals(int.class) || clazz.isAssignableFrom(Integer.class))
        {
            return Bytes.toBytes(value instanceof Integer ? (Integer) value : new Integer(value.toString()));
        }
        else if (clazz.equals(long.class) || clazz.isAssignableFrom(Long.class))
        {
            return Bytes.toBytes(value instanceof Long ? (Long) value : new Long(value.toString()));
        }
        else if (clazz.equals(boolean.class) || clazz.isAssignableFrom(Boolean.class))
        {
            return Bytes.toBytes(value instanceof Boolean ? (Boolean) value : new Boolean(value.toString()));
        }
        else if (clazz.equals(double.class) || clazz.isAssignableFrom(Double.class))
        {
            return Bytes.toBytes(value instanceof Double ? (Double) value : new Double(value.toString()));
        }
        // else if (clazz.isAssignableFrom(java.util.UUID.class))
        // {
        // return Bytes.toBytes(value.toString());
        // }
        else if (clazz.equals(float.class) || clazz.isAssignableFrom(Float.class))
        {
            return Bytes.toBytes(value instanceof Float ? (Float) value : new Float(value.toString()));
        }
        else if (clazz.equals(short.class) || clazz.isAssignableFrom(Short.class))
        {
            return Bytes.toBytes(value instanceof Short ? (Short) value : new Short(value.toString()));
        }
        else if (clazz.equals(BigDecimal.class))
        {
            return Bytes.toBytes(value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString()));
        }
        else
        {
            if (value.getClass().isAssignableFrom(String.class))
            {
                value = PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz, value.toString());
            }
            return PropertyAccessorFactory.getPropertyAccessor(clazz).toBytes(value);
        }
    }

    /**
     * Returns bytes value for given value.
     * 
     * @param o
     *            the o
     * @return bytes value.
     */
    public static byte[] getBytes(Object o)
    {
        if (o != null)
        {
            return getBytes(o, o.getClass());
        }

        return null;
    }

    /**
     * From bytes.
     * 
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param b
     *            the b
     * @return the object
     */
    public static Object fromBytes(EntityMetadata m, MetamodelImpl metaModel, byte[] b)
    {
        Class idFieldClass = m.getIdAttribute().getJavaType();
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            return fromBytes(b, String.class);
        }
        return fromBytes(b, idFieldClass);
    }

    /**
     * From bytes.
     * 
     * @param b
     *            the b
     * @param clazz
     *            the clazz
     * @return the object
     */
    public static Object fromBytes(byte[] b, Class<?> clazz)
    {

        if (clazz.isAssignableFrom(String.class))
        {
            return Bytes.toString(b);
        }
        else if (clazz.equals(int.class) || clazz.isAssignableFrom(Integer.class))
        {
            return Bytes.toInt(b);
        }
        else if (clazz.equals(long.class) || clazz.isAssignableFrom(Long.class))
        {
            return Bytes.toLong(b);
        }
        else if (clazz.equals(boolean.class) || clazz.isAssignableFrom(Boolean.class))
        {
            return Bytes.toBoolean(b);
        }
        else if (clazz.equals(double.class) || clazz.isAssignableFrom(Double.class))
        {
            return Bytes.toDouble(b);
        }
        else if (clazz.equals(float.class) || clazz.isAssignableFrom(Float.class))
        {
            return Bytes.toFloat(b);
        }
        else if (clazz.equals(short.class) || clazz.isAssignableFrom(Short.class))
        {
            return Bytes.toShort(b);
        }
        else if (clazz.equals(BigDecimal.class))
        {
            return Bytes.toBigDecimal(b);
        }
        else
        {
            return PropertyAccessorFactory.getPropertyAccessor(clazz).fromBytes(clazz, b);
        }
    }

    /**
     * Gets the operator.
     * 
     * @param condition
     *            the condition
     * @param idPresent
     *            the id present
     * @param useFilter
     *            the use filter
     * @return the operator
     */
    public static SingleColumnFilterFactory getOperator(String condition, boolean idPresent, boolean useFilter)
    {
        if (condition.equals("="))
        {
            return SingleColumnFilterFactory.EQUAL;
        }
        else if (condition.equals(">"))
        {
            return SingleColumnFilterFactory.GREATER;
        }
        else if (condition.equals("<"))
        {
            return SingleColumnFilterFactory.LESS;
        }
        else if (condition.equals(">="))
        {
            return SingleColumnFilterFactory.GREATER_OR_EQUAL;
        }
        else if (condition.equals("<="))
        {
            return SingleColumnFilterFactory.LESS_OR_EQUAL;
        }
        else if (condition.equals("LIKE"))
        {
            return SingleColumnFilterFactory.LIKE;
        }
        else if (condition.equals("REGEXP"))
        {
            return SingleColumnFilterFactory.REGEXP;
        }
        else if (useFilter)
        {
            if (!idPresent)
            {
                throw new UnsupportedOperationException(" Condition " + condition + " is not suported in  hbase!");
            }
            else
            {
                throw new UnsupportedOperationException(" Condition " + condition
                        + " is not suported for query on row key!");

            }
        }
        return null;
    }

}