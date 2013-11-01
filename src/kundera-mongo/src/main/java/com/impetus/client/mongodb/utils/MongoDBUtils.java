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
import java.util.Map;
import java.util.Properties;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.KunderaAuthenticationException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.ReflectUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
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
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                Attribute compositeColumn = compoundKey.getAttribute(f.getName());

                compoundKeyObj.put(
                        ((AbstractAttribute) compositeColumn).getJPAColumnName(),
                        populateValue(PropertyAccessorHelper.getObject(id, (Field) compositeColumn.getJavaMember()),
                                ((AbstractAttribute) compositeColumn).getBindableJavaType()));
            }
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
                        || (clazz.isAssignableFrom(char.class)) || (clazz.isAssignableFrom(Character.class)));
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

    /**
     * Method to authenticate connection with mongodb. throws runtime error if:
     * a) userName and password, any one is not null. b) if authentication
     * fails.
     * 
     * 
     * @param props
     *            persistence properties.
     * @param externalProperties
     *            external persistence properties.
     * @param mongoDB
     *            mongo db connection.
     */
    public static void authenticate(Properties props, Map<String, Object> externalProperties, DB mongoDB)
    {
        String password = null;
        String userName = null;
        if (externalProperties != null)
        {
            userName = (String) externalProperties.get(PersistenceProperties.KUNDERA_USERNAME);
            password = (String) externalProperties.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        if (userName == null)
        {
            userName = (String) props.get(PersistenceProperties.KUNDERA_USERNAME);
        }
        if (password == null)
        {
            password = (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);
        }
        boolean authenticate = true;
        String errMsg = null;
        if (userName != null && password != null)
        {
            authenticate = mongoDB.authenticate(userName, password.toCharArray());
        }
        else if ((userName != null && password == null) || (userName == null && password != null))
        {
            errMsg = "Invalid configuration provided for authentication, please specify both non-nullable"
                    + " 'kundera.username' and 'kundera.password' properties";
            throw new ClientLoaderException(errMsg);
        }

        if (!authenticate)
        {
            errMsg = "Authentication failed, invalid 'kundera.username' :" + userName + "and 'kundera.password' :"
                    + password + " provided";
            throw new KunderaAuthenticationException(errMsg);
        }
    }
}
