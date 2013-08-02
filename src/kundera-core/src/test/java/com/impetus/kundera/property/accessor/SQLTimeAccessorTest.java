/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.property.accessor;

import java.sql.Time;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 * 
 */
public class SQLTimeAccessorTest
{
    PropertyAccessor<Time> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new SQLTimeAccessor();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        accessor = null;
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(Time.class, null));

        long l = System.currentTimeMillis();
        Time d = new Time(l);
        byte[] b = accessor.toBytes(d);

        Time d2 = accessor.fromBytes(Time.class, b);

        Assert.assertEquals(d, d2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));

        long l = System.currentTimeMillis();
        Time d = new Time(l);
        byte[] b = accessor.toBytes(d);

        Time d2 = accessor.fromBytes(Time.class, b);

        Assert.assertEquals(d, d2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));

        long l = System.currentTimeMillis();
        Time d = new Time(l);

        Assert.assertEquals("" + l, accessor.toString(d));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Time.class, null));

        long l = System.currentTimeMillis();
        Time d = new Time(l);
        Time d2 = accessor.fromString(Time.class, "" + l);
        Assert.assertEquals(d, d2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#getCopy(java.lang.Object)}
     * .
     */
    @Test
    public void testGetCopy()
    {
        long l = System.currentTimeMillis();
        Time d = new Time(l);

        Time d2 = accessor.getCopy(d);
        Assert.assertNotNull(d2);
        Assert.assertTrue(d.equals(d2));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#getInstance(java.lang.Class)}
     * .
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Time.class);
        Assert.assertNotNull(o);

        Assert.assertEquals(Integer.MAX_VALUE, ((Time) o).getTime());
    }

}
