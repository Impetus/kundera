package com.impetus.client.hbase.utils;

import java.math.BigDecimal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;

public final class HBaseUtils
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseUtils.class);

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
//    public static byte[] getBytes(String fieldName, EntityMetadata m, Object value)
//    {
//        String idColName = m.getIdAttribute().getName();
//        Field f = null;
//        // boolean isId = false;
//        if (idColName.equals(fieldName))
//        {
//            f = (Field) m.getIdAttribute().getJavaMember();
//            // isId = true;
//        }
//        else
//        {
//            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
//                    m.getPersistenceUnit());
//
//            EntityType entityType = metaModel.entity(m.getEntityClazz());
//
//            Attribute a = entityType.getAttribute(fieldName);
//
//            if (a == null)
//            {
//                throw new QueryHandlerException("column type is null for: " + fieldName);
//            }
//            f = (Field) a.getJavaMember();
//        }
//
//        if (f != null && f.getType() != null)
//        {
//            return getBytes(value, f.getType());
//        }
//        else
//        {
//            log.error("Error while handling data type for:" + fieldName);
//            throw new QueryHandlerException("field type is null for:" + fieldName);
//        }
//    }

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
            return Bytes.toBytes(Integer.parseInt(value.toString()));
        }
        else if (clazz.equals(long.class) || clazz.isAssignableFrom(Long.class))
        {
            return Bytes.toBytes(Long.parseLong(value.toString()));
        }
        else if (clazz.equals(boolean.class) || clazz.isAssignableFrom(Boolean.class))
        {
            return Bytes.toBytes(Boolean.valueOf(value.toString()));
        }
        else if (clazz.equals(double.class) || clazz.isAssignableFrom(Double.class))
        {
            return Bytes.toBytes(Double.valueOf(value.toString()));
        }
        // else if (clazz.isAssignableFrom(java.util.UUID.class))
        // {
        // return Bytes.toBytes(value.toString());
        // }
        else if (clazz.equals(float.class) || clazz.isAssignableFrom(Float.class))
        {
            return Bytes.toBytes(Float.valueOf(value.toString()));
        }
        else if (clazz.equals(short.class) || clazz.isAssignableFrom(Short.class))
        {
            return Bytes.toBytes(Short.valueOf(value.toString()));
        }
        else if (clazz.equals(BigDecimal.class))
        {
            return Bytes.toBytes(BigDecimal.valueOf(new Long(value.toString())));
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
        return getBytes(o, o.getClass());
    }

    public static Object fromBytes(EntityMetadata m, byte[] b)
    {
        Class idFieldClass = m.getIdAttribute().getJavaType();
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
        // else if (clazz.isAssignableFrom(java.util.UUID.class))
        // {
        // return Bytes.toBytes(b.toString());
        // }
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
    public static CompareOp getOperator(String condition, boolean idPresent)
    {
        if (/* !idPresent && */condition.equals("="))
        {
            return CompareOp.EQUAL;
        }
        else if (/* !idPresent && */condition.equals(">"))
        {
            return CompareOp.GREATER;
        }
        else if (/* !idPresent && */condition.equals("<"))
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
        else
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

    }

}