/**
 * 
 */
package com.impetus.client.mongodb.utils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Properties;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The Class MongoDBUtils.
 * 
 * @author Kuldeep Mishra
 */
public class MongoDBUtils
{
    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(MongoDBUtils.class);

    /** The Constant METADATA. */
    public static final String METADATA = "metadata";

    /** The Constant FILES. */
    public static final String FILES = ".files";

    /** The Constant CHUNKS. */
    public static final String CHUNKS = ".chunks";

    /**
     * Populate compound key.
     * 
     * @param dbObj
     *            the db obj
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param id
     *            the id
     */
    public static void populateCompoundKey(DBObject dbObj, EntityMetadata m, MetamodelImpl metaModel, Object id)
    {
        EmbeddableType compoundKey = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
        // Iterator<Attribute> iter = compoundKey.getAttributes().iterator();
        BasicDBObject compoundKeyObj = new BasicDBObject();

        compoundKeyObj = getCompoundKeyColumns(m, id, compoundKey);

        dbObj.put("_id", compoundKeyObj);
    }

    /**
     * Gets the compound key columns.
     * 
     * @param m
     *            the m
     * @param id
     *            the id
     * @param compoundKey
     *            the compound key
     * @return the compound key columns
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
     * Populate value.
     * 
     * @param valObj
     *            the val obj
     * @param clazz
     *            the clazz
     * @return the object
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

    /**
     * Checks if is UT f8 value.
     * 
     * @param clazz
     *            the clazz
     * @return true, if is UT f8 value
     */
    private static boolean isUTF8Value(Class<?> clazz)
    {
        return (clazz.isAssignableFrom(BigDecimal.class))
                || (clazz.isAssignableFrom(BigInteger.class) || (clazz.isAssignableFrom(String.class))
                        || (clazz.isAssignableFrom(char.class)) || (clazz.isAssignableFrom(Character.class)));
    }

    /**
     * Gets the translated object.
     * 
     * @param value
     *            the value
     * @param sourceClass
     *            the source class
     * @param targetClass
     *            the target class
     * @return the translated object
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

    /**
     * Gets the DB object.
     * 
     * @param m
     *            the m
     * @param tableName
     *            the table name
     * @param dbObjects
     *            the db objects
     * @param metaModel
     *            the meta model
     * @param id
     *            the id
     * @return the DB object
     */
    public static DBObject getDBObject(EntityMetadata m, String tableName, Map<String, DBObject> dbObjects,
            MetamodelImpl metaModel, Object id)
    {
        tableName = tableName != null ? tableName : m.getTableName();
        DBObject dbObj = dbObjects.get(tableName);
        if (dbObj == null)
        {
            dbObj = new BasicDBObject();
            dbObjects.put(tableName, dbObj);
        }

        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(dbObj, m, metaModel, id);
        }
        else
        {
            dbObj.put("_id", MongoDBUtils.populateValue(id, id.getClass()));
        }
        return dbObj;
    }

    /**
     * Calculate m d5.
     * 
     * @param val
     *            the val
     * @return the string
     */
    public static String calculateMD5(Object val)
    {
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            logger.error("Unable to calculate MD5 for file, Caused By: ", e);
        }
        md.update((byte[]) val);

        byte[] digest = md.digest();
        return DatatypeConverter.printHexBinary(digest).toLowerCase();
    }
}
