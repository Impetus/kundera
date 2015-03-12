package com.impetus.client.hbase.utils;

import java.math.BigDecimal;

import javax.persistence.metamodel.Metamodel;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.utils.KunderaCoreUtils;

public final class HBaseUtils
{
    /**
     * @param value
     * @param clazz
     * @return
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
     * @param fieldName
     *            field name.
     * @param m
     *            entity metadata
     * @param value
     *            value.
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

    public static Object fromBytes(EntityMetadata m, MetamodelImpl metaModel, byte[] b)
    {
        Class idFieldClass = m.getIdAttribute().getJavaType();
        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            return fromBytes(b, String.class);
        }
        return fromBytes(b, idFieldClass);
    }

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
     * @return the operator
     */
    public static CompareOp getOperator(String condition, boolean idPresent, boolean useFilter)
    {
        if (condition.equals("="))
        {
            return CompareOp.EQUAL;
        }
        else if (condition.equals(">"))
        {
            return CompareOp.GREATER;
        }
        else if (condition.equals("<"))
        {
            return CompareOp.LESS;
        }
        else if (condition.equals(">="))
        {
            return CompareOp.GREATER_OR_EQUAL;
        }
        else if (condition.equals("<="))
        {
            return CompareOp.LESS_OR_EQUAL;
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