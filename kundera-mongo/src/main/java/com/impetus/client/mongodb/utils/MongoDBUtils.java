/**
 * 
 */
package com.impetus.client.mongodb.utils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBUtils
{
    public static void populateCompoundKey(DBObject dbObj, EntityMetadata m, MetamodelImpl metaModel, Object id)
    {
        EmbeddableType compoundKey = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
        // Iterator<Attribute> iter = compoundKey.getAttributes().iterator();
        BasicDBObject compoundKeyObj = new BasicDBObject();

        compoundKeyObj = getCompoundKeyColumns(m, id, compoundKey);

        dbObj.put("_id", compoundKeyObj);
    }

    /**
     * @param m
     * @param id
     * @param compoundKey
     * @param compoundKeyObj
     */
    public static BasicDBObject getCompoundKeyColumns(EntityMetadata m, Object id, EmbeddableType compoundKey)
    {
        BasicDBObject compoundKeyObj = new BasicDBObject();
        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();

        // To ensure order.
        for (Field f : fields)
        {
            Attribute compositeColumn = compoundKey.getAttribute(f.getName());

            compoundKeyObj.put(
                    ((AbstractAttribute) compositeColumn).getJPAColumnName(),
                    populateValue(PropertyAccessorHelper.getObject(id, (Field) compositeColumn.getJavaMember()),
                            ((AbstractAttribute) compositeColumn).getBindableJavaType()));
        }

        return compoundKeyObj;
    }

    /**
     * @param valObj
     * @return
     */
    public static Object populateValue(Object valObj, Class clazz)
    {
        if (isUTF8Value(clazz) || clazz.isEnum())
        {
            return valObj.toString();
        }
        else if ((valObj instanceof Calendar) || (valObj instanceof GregorianCalendar))
        {
            return ((Calendar) valObj).getTime();
        }
        return valObj;
    }

    private static boolean isUTF8Value(Class<?> clazz)
    {
        return (clazz.isAssignableFrom(BigDecimal.class))
                || (clazz.isAssignableFrom(BigInteger.class) || (clazz.isAssignableFrom(String.class))
                /*
                 * || (clazz.isAssignableFrom(Calendar.class)) ||
                 * (clazz.isAssignableFrom(GregorianCalendar.class))
                 */);
    }

    /**
     * @param value
     * @param sourceClass
     * @param targetClass
     * @return
     */
    public static Object getTranslatedObject(Object value, Class<?> sourceClass, Class<?> targetClass)
    {
        if (sourceClass.isAssignableFrom(Date.class))
        {
            value = PropertyAccessorHelper.fromDate(targetClass, sourceClass, value);
        }
        else
        {
            value = PropertyAccessorHelper.fromSourceToTargetClass(targetClass, sourceClass, value);
        }
        return value;
    }
}
