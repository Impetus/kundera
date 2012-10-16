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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Provides functionality for mapping between MongoDB documents and POJOs.
 * Contains utility methods for converting one form into another.
 * 
 * @author amresh.singh
 */
public class DocumentObjectMapper
{

    /** The log. */
    private static Log log = LogFactory.getLog(DocumentObjectMapper.class);

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
    public static BasicDBObject getDocumentFromObject(Object obj, Set<Attribute> columns)
            throws PropertyAccessException
    {
        BasicDBObject dBObj = new BasicDBObject();

        // for (Column column : columns)
        for (Attribute column : columns)
        {
            Field f = (Field) column.getJavaMember();
            // TODO: This is not a good logic and need to be modified
            if (f.getType().isPrimitive() || f.getType().equals(String.class) || f.getType().equals(Integer.class)
                    || f.getType().equals(Long.class) || f.getType().equals(Short.class)
                    || f.getType().equals(Float.class) || f.getType().equals(Double.class))
            {
                Object val = PropertyAccessorHelper.getObject(obj, f);
                dBObj.put(column.getName(), val);
            }
            else if (f.getType().isAssignableFrom(Point.class))
            {
                Point p = (Point) PropertyAccessorHelper.getObject(obj, (Field) column.getJavaMember());
                if (p != null)
                {
                    double[] coordinate = new double[] { p.getX(), p.getY() };
                    dBObj.put(((AbstractAttribute) column).getJPAColumnName(), coordinate);
                }
            }
            else
            {
                log.warn("Field " + f.getName()
                        + " is not a premitive, String or Wrapper object, and hence, won't be part of persistence");
            }

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
    public static BasicDBObject[] getDocumentListFromCollection(Collection coll, Set<Attribute> columns)
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
    public static Object getObjectFromDocument(BasicDBObject documentObj, Class clazz, Set<Attribute> columns)
    {
        try
        {
            Object obj = clazz.newInstance();
            // for (Column column : columns)
            for (Attribute column : columns)
            {
                Object val = documentObj.get(((AbstractAttribute) column).getJPAColumnName());
                if (column.getJavaType().isAssignableFrom(Point.class))
                {
                    Point point = null;
                    if(val != null)
                    {
                        BasicDBList list = (BasicDBList) val;
                        double x = Double.parseDouble(list.get(0).toString());
                        double y = Double.parseDouble(list.get(1).toString());
                        point = new Point(x, y);
                    }                    
                    PropertyAccessorHelper.set(obj, (Field) column.getJavaMember(), point);
                }
                else
                {
                    PropertyAccessorHelper.set(obj, (Field) column.getJavaMember(), val);
                }
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
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e);
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
    public static Collection<?> getCollectionFromDocumentList(BasicDBList documentList, Class embeddedCollectionClass,
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

}
