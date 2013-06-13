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
package com.impetus.client.mongodb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.PluralAttribute.CollectionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Provides functionality for mapping between MongoDB documents and POJOs.
 * Contains utility methods for converting one form into another.
 * 
 * @author amresh.singh
 */
public class DocumentObjectMapper
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(DocumentObjectMapper.class);

    /**
     * Creates a MongoDB document object wrt a given Java object. columns in the
     * document correspond Columns provided as List.
     * 
     * @param obj
     *            the obj
     * @param columns
     *            the columns
     * @return the document from object
     * @throws PropertyAccessException
     *             the property access exception
     */
    static BasicDBObject getDocumentFromObject(Object obj, Set<Attribute> columns) throws PropertyAccessException
    {
        BasicDBObject dBObj = new BasicDBObject();

        for (Attribute column : columns)
        {
            extractEntityField(obj, dBObj, column);
        }
        return dBObj;
    }

    /**
     * Creates a MongoDB document list from a given java collection. columns in
     * the document correspond Columns provided as List.
     * 
     * @param coll
     *            the coll
     * @param columns
     *            the columns
     * @return the document list from collection
     * @throws PropertyAccessException
     *             the property access exception
     */
    static BasicDBObject[] getDocumentListFromCollection(Collection coll, Set<Attribute> columns)
            throws PropertyAccessException
    {
        BasicDBObject[] dBObjects = new BasicDBObject[coll.size()];
        int count = 0;
        for (Object o : coll)
        {
            dBObjects[count] = getDocumentFromObject(o, columns);
            count++;
        }
        return dBObjects;
    }

    /**
     * Creates an instance of <code>clazz</code> and populates fields fetched
     * from MongoDB document object. Field names are determined from
     * <code>columns</code>
     * 
     * @param documentObj
     *            the document obj
     * @param clazz
     *            the clazz
     * @param columns
     *            the columns
     * @return the object from document
     */
    static Object getObjectFromDocument(BasicDBObject documentObj, Class clazz, Set<Attribute> columns)
    {
        try
        {
            Object obj = clazz.newInstance();
            for (Attribute column : columns)
            {
                setColumnValue(documentObj, obj, column);
            }
            return obj;
        }
        catch (InstantiationException e)
        {
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new PersistenceException(e);
        }
    }

    /**
     * Setter for column value, by default converted from string value, in case
     * of map it is automatically converted into map using BasicDBObject.
     * 
     * @param document
     *            mongo document
     * @param entityObject
     *            searched entity.
     * @param column
     *            column field.
     */
    static void setColumnValue(DBObject document, Object entityObject, Attribute column)
    {
        Object value = document.get(((AbstractAttribute) column).getJPAColumnName());
        if (value != null)
        {
            Class javaType = column.getJavaType();
            try
            {
                switch (JavaType.getJavaType(javaType))
                {
                case MAP:
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(),
                            ((BasicDBObject) value).toMap());
                    break;
                case SET:
                    List collectionValues = Arrays.asList(((BasicDBList) value).toArray());
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(), new HashSet(
                            collectionValues));
                    break;
                case LIST:
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(),
                            Arrays.asList(((BasicDBList) value).toArray()));
                    break;
                case POINT:

                    BasicDBList list = (BasicDBList) value;

                    Object xObj = list.get(0);
                    Object yObj = list.get(1);

                    if (xObj != null && yObj != null)
                    {
                        try
                        {
                            double x = Double.parseDouble(xObj.toString());
                            double y = Double.parseDouble(yObj.toString());

                            Point point = new Point(x, y);
                            PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(), point);
                        }
                        catch (NumberFormatException e)
                        {
                            log.error("Error while reading geolocation data for column " + column
                                    + "; Reason - possible corrupt data. " + e.getMessage());
                            throw new EntityReaderException("Error while reading geolocation data for column " + column
                                    + "; Reason - possible corrupt data.", e);
                        }
                    }
                    break;
                case ENUM:
                    EnumAccessor accessor = new EnumAccessor();
                    value = accessor.fromString(javaType, value.toString());
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(), value);
                    break;
                case PRIMITIVE:
                    value = MongoDBUtils.populateValue(value, value.getClass());
                    value = MongoDBUtils.getTranslatedObject(value, value.getClass(), javaType);
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(), value);
                    break;
                }
            }
            catch (PropertyAccessException e)
            {
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Extract entity field.
     * 
     * @param entity
     *            the entity
     * @param dbObj
     *            the db obj
     * @param column
     *            the column
     * @throws PropertyAccessException
     *             the property access exception
     */
    static void extractEntityField(Object entity, DBObject dbObj, Attribute column) throws PropertyAccessException
    {
        Object valueObject = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());

        if (valueObject != null)
        {
            Class javaType = column.getJavaType();
            try
            {
                switch (JavaType.getJavaType(javaType))
                {
                case MAP:
                    Map mapObj = (Map) valueObject;
                    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start(mapObj);
                    dbObj.put(((AbstractAttribute) column).getJPAColumnName(), builder.get());
                    break;
                case SET:
                case LIST:
                    Collection collection = (Collection) valueObject;
                    BasicDBList basicDBList = new BasicDBList();
                    for (Object o : collection)
                    {
                        basicDBList.add(o);
                    }
                    dbObj.put(((AbstractAttribute) column).getJPAColumnName(), basicDBList);
                    break;
                case POINT:

                    Point p = (Point) valueObject;
                    double[] coordinate = new double[] { p.getX(), p.getY() };
                    dbObj.put(((AbstractAttribute) column).getJPAColumnName(), coordinate);
                    break;
                case ENUM:
                case PRIMITIVE:
                    dbObj.put(((AbstractAttribute) column).getJPAColumnName(),
                            MongoDBUtils.populateValue(valueObject, javaType));
                    break;
                }
            }
            catch (PropertyAccessException e)
            {
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Creates a collection of <code>embeddedObjectClass</code> instances
     * wherein each element is java object representation of MongoDB document
     * object contained in <code>documentList</code>. Field names are determined
     * from <code>columns</code>.
     * 
     * @param documentList
     *            the document list
     * @param embeddedCollectionClass
     *            the embedded collection class
     * @param embeddedObjectClass
     *            the embedded object class
     * @param columns
     *            the columns
     * @return the collection from document list
     */
    static Collection<?> getCollectionFromDocumentList(BasicDBList documentList, Class embeddedCollectionClass,
            Class embeddedObjectClass, Set<Attribute> columns)
    {
        Collection<Object> embeddedCollection = null;
        if (embeddedCollectionClass.equals(Set.class))
        {
            embeddedCollection = new HashSet<Object>();
        }
        else if (embeddedCollectionClass.equals(List.class))
        {
            embeddedCollection = new ArrayList<Object>();
        }
        else
        {
            throw new PersistenceException("Invalid collection class " + embeddedCollectionClass
                    + "; only Set and List allowed");
        }

        for (Object dbObj : documentList)
        {
            embeddedCollection.add(getObjectFromDocument((BasicDBObject) dbObj, embeddedObjectClass, columns));
        }

        return embeddedCollection;
    }

    private enum JavaType
    {
        ENUM, LIST, SET, MAP, POINT, PRIMITIVE;

        private static JavaType getJavaType(Class javaType)
        {
            JavaType type = null;
            if (javaType.isAssignableFrom(List.class))
            {
                type = LIST;
            }
            else if (javaType.isAssignableFrom(Map.class))
            {
                type = MAP;
            }
            else if (javaType.isAssignableFrom(Set.class))
            {
                type = SET;
            }
            else if (javaType.isEnum())
            {
                type = ENUM;
            }
            else if (javaType.isAssignableFrom(Point.class))
            {
                type = POINT;
            }
            else
            {
                type = PRIMITIVE;
            }
            return type;
        }
    }
}