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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.property.PropertyAccessor;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Test case for {@link PropertyAccessor}
 * 
 * @author amresh.singh
 */
public class PointAccessorTest
{
    PointAccessor pa;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        pa = new PointAccessor();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        pa = null;
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        Point point = new Point(4.5, 6.3);
        byte[] input = pa.toBytes(point);

        Point point2 = pa.fromBytes(Point.class, input);
        Assert.assertTrue(point.equals(point2));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        Point point = new Point(4.5, 6.3);
        byte[] input = pa.toBytes(point);

        Point point2 = pa.fromBytes(Point.class, input);
        Assert.assertTrue(point.equals(point2));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        Point point = new Point(4.5, 6.3);
        String pointStr = pa.toString(point);

        WKTWriter writer = new WKTWriter();
        String wktStr = writer.write(point);

        Assert.assertEquals(wktStr, pointStr);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        Point point = new Point(4.5, 6.3);
        WKTWriter writer = new WKTWriter();
        String wktStr = writer.write(point);

        Point point2 = pa.fromString(com.vividsolutions.jts.geom.Point.class, wktStr);

        Assert.assertNotNull(point2);
        Assert.assertEquals(point.getX(), point2.getX());
        Assert.assertEquals(point.getY(), point2.getY());
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#getCopy(java.lang.Object)}
     * .
     */
    @Test
    public void testGetCopy()
    {
        Point point = new Point(4.5, 6.3);
        Point point2 = pa.getCopy(point);
        Assert.assertFalse(point == point2);
        Assert.assertTrue(point.equals(point2));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.PointAccessor#getInstance(java.lang.Class)}
     * .
     */
    @Test
    public void testGetInstance()
    {
        Point point = (Point) pa.getInstance(Point.class);
        Assert.assertNotNull(point);
        Assert.assertEquals(0.0, point.getX());
        Assert.assertEquals(0.0, point.getY());
    }

}
