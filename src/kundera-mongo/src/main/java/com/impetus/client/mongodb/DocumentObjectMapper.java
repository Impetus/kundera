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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.attributes.AttributeType;
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
    static Map<String, DBObject> getDocumentFromObject(Metamodel metaModel, Object obj, Set<Attribute> columns,
            String tableName) throws PropertyAccessException
    {
        Map<String, DBObject> embeddedObjects = new HashMap<String, DBObject>();
        // BasicDBObject dBObj = new BasicDBObject();

        for (Attribute column : columns)
        {
            String collectionName = ((AbstractAttribute) column).getTableName() != null
                    ? ((AbstractAttribute) column).getTableName() : tableName;

            DBObject dbObject = embeddedObjects.get(collectionName);
            if (dbObject == null)
            {
                dbObject = new BasicDBObject();
                embeddedObjects.put(collectionName, dbObject);
            }

            if (((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) column).getBindableJavaType()))
            {
                DefaultMongoDBDataHandler handler = new DefaultMongoDBDataHandler();
                // handler.onEmbeddable(column, obj, metaModel, dBObj,
                // collectionName);
                handler.onEmbeddable(column, obj, metaModel, dbObject, collectionName);
            }
            else
            {
                extractFieldValue(obj, dbObject, column);
            }
        }
        return embeddedObjects;
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
    static BasicDBObject[] getDocumentListFromCollection(Metamodel metaModel, Collection coll, Set<Attribute> columns,
            String tableName) throws PropertyAccessException
    {
        BasicDBObject[] dBObjects = new BasicDBObject[coll.size()];
        int count = 0;
        for (Object o : coll)
        {
            dBObjects[count] = (BasicDBObject) getDocumentFromObject(metaModel, o, columns, tableName).values()
                    .toArray()[0];
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
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    static Object getObjectFromDocument(Metamodel metamodel, BasicDBObject documentObj, Set<Attribute> columns,
            Object obj) throws InstantiationException, IllegalAccessException
    {
        // try
        // {
        // Object obj = clazz.newInstance();
        for (Attribute column : columns)
        {
            if (((MetamodelImpl) metamodel).isEmbeddable(((AbstractAttribute) column).getBindableJavaType()))
            {
                DefaultMongoDBDataHandler handler = new DefaultMongoDBDataHandler();
                handler.onViaEmbeddable(column, obj, metamodel, documentObj);
            }
            else
            {
                setFieldValue(documentObj, obj, column, false);
            }
        }
        return obj;
        // }
        // catch (InstantiationException e)
        // {
        // throw new PersistenceException(e);
        // }
        // catch (IllegalAccessException e)
        // {
        // throw new PersistenceException(e);
        // }
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
    static void setFieldValue(DBObject document, Object entityObject, Attribute column, boolean isLob)
    {
        Object value = null;

        if (document != null)
        {
            value = isLob ? ((DBObject) document.get("metadata")).get(((AbstractAttribute) column).getJPAColumnName())
                    : document.get(((AbstractAttribute) column).getJPAColumnName());
        }
        if (value != null)
        {
            Class javaType = column.getJavaType();
            try
            {
                switch (AttributeType.getType(javaType))
                {
                case MAP:
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(),
                            ((BasicDBObject) value).toMap());
                    break;
                case SET:
                    List collectionValues = Arrays.asList(((BasicDBList) value).toArray());
                    PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(),
                            new HashSet(collectionValues));
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
                            log.error(
                                    "Error while reading geolocation data for column {} ; Reason - possible corrupt data, Caused by : .",
                                    column, e);
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
            catch (PropertyAccessException paex)
            {
                log.error("Error while setting column {} value, caused by : .",
                        ((AbstractAttribute) column).getJPAColumnName(), paex);
                throw new PersistenceException(paex);
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
    static void extractFieldValue(Object entity, DBObject dbObj, Attribute column) throws PropertyAccessException
    {
        try
        {
            Object valueObject = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());

            if (valueObject != null)
            {
                Class javaType = column.getJavaType();
                switch (AttributeType.getType(javaType))
                {
                case MAP:
                    Map mapObj = (Map) valueObject;
                    // BasicDBObjectBuilder builder =
                    // BasicDBObjectBuilder.start(mapObj);
                    BasicDBObjectBuilder b = new BasicDBObjectBuilder();
                    Iterator i = mapObj.entrySet().iterator();
                    while (i.hasNext())
                    {
                        Map.Entry entry = (Map.Entry) i.next();
                        b.add(entry.getKey().toString(),
                                MongoDBUtils.populateValue(entry.getValue(), entry.getValue().getClass()));
                    }
                    dbObj.put(((AbstractAttribute) column).getJPAColumnName(), b.get());
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
        }
        catch (PropertyAccessException paex)
        {
            log.error("Error while getting column {} value, caused by : .",
                    ((AbstractAttribute) column).getJPAColumnName(), paex);
            throw new PersistenceException(paex);
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
     * @param metamodel
     * @return the collection from document list
     */
    static Collection<?> getCollectionFromDocumentList(Metamodel metamodel, BasicDBList documentList,
            Class embeddedCollectionClass, Class embeddedObjectClass, Set<Attribute> columns)
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
            throw new PersistenceException(
                    "Invalid collection class " + embeddedCollectionClass + "; only Set and List allowed");
        }

        for (Object dbObj : documentList)
        {
            try
            {
                Object obj = embeddedObjectClass.newInstance();
                embeddedCollection.add(getObjectFromDocument(metamodel, (BasicDBObject) dbObj, columns, obj));
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

        return embeddedCollection;
    }
}