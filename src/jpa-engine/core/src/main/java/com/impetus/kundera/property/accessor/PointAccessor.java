/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.property.accessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Property Accessor for {@link Point}
 * 
 * @author amresh.singh
 */
public class PointAccessor implements PropertyAccessor<Point>
{
    public static Logger log = LoggerFactory.getLogger(PointAccessor.class);
    
    @Override
    public Point fromBytes(Class targetClass, byte[] b)
    {
        ObjectInputStream ois;
        Point o;
        try
        {
            ois = new ObjectInputStream(new ByteArrayInputStream(b));
            o = (Point) ois.readObject();
            ois.close();
        }
        catch (IOException e)
        {
            log.error("IO exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
        catch (ClassNotFoundException e)
        {
            log.error("Class not found exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
        return o;
    }

    @Override
    public byte[] toBytes(Object object)
    {
        ByteArrayOutputStream baos;
        try
        {
            baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            log.error("IO exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public String toString(Object object)
    {
        return object != null ? object.toString() : null;
    }

    @Override
    public Point fromString(Class targetClass, String s)
    {
        if (s == null)
            return null;

        WKTReader reader = new WKTReader();
        try
        {
            return new Point((com.vividsolutions.jts.geom.Point) reader.read(s));
        }
        catch (ParseException e)
        {
            log.error("Parse exception, Caused by {}.", e);
            throw new PropertyAccessException(e);
        }
    }

    @Override
    public Point getCopy(Object object)
    {
        if (object == null)
            return null;

        Point p = (Point) object;
        return new Point(p.getX(), p.getY());
    }

    @Override
    public Object getInstance(Class<?> clazz)
    {
        return new Point(0.0, 0.0);
    }

}
