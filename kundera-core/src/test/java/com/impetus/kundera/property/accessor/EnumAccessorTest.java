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
package com.impetus.kundera.property.accessor;

import java.io.UnsupportedEncodingException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;

/**
 * @author amresh.singh
 * 
 */
public class EnumAccessorTest
{

    enum Day
    {
        MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
    }

    Day day1 = Day.MONDAY;

    Day day2 = Day.TUESDAY;

    EnumAccessor accessor = new EnumAccessor();

    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromBytes(byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        try
        {
            byte[] b = day1.name().getBytes(Constants.ENCODING);

            Day dd = (Day) accessor.fromBytes(Day.class, b);

            Assert.assertEquals(day1, dd);

            Object o = accessor.fromBytes(Day.class, null);

            Assert.assertNull(o);

        }
        catch (UnsupportedEncodingException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        try
        {
            byte[] b = accessor.toBytes(day1);
            String s = new String(b, Constants.ENCODING);
            Assert.assertEquals(day1.MONDAY.name(), s);

            b = accessor.toBytes(null);

            Assert.assertNull(b);

        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (UnsupportedEncodingException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        String s = accessor.toString(day1);
        Assert.assertEquals(Day.MONDAY.name(), s);

        s = accessor.toString(null);
        Assert.assertNull(s);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromString(java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        try
        {
            Day day11 = (Day) accessor.fromString(Day.class, day1.name());
            Assert.assertEquals(day1, day11);

            Day day22 = (Day) accessor.fromString(Day.class, day2.name());
            Assert.assertEquals(day2, day22);

            Day day = (Day) accessor.fromString(Day.class, null);
            Assert.assertNull(day);

            day = (Day) accessor.fromString(null, day1.name());
            Assert.assertNull(day);

            day = (Day) accessor.fromString(null, null);
            Assert.assertNull(day);
        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromString(java.lang.String)}
     * .
     */
    @Test
    public void testGetCopy()
    {
        try
        {
            Day day = Day.MONDAY;
            Day d = (Day) accessor.getCopy(day);
            Assert.assertNotNull(d);
            Assert.assertTrue(day.equals(d));

            d = (Day) accessor.getCopy(null);
            Assert.assertNull(d);
        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.EnumAccessor#fromString(java.lang.String)}
     * .
     */
    @Test
    public void testGetInstance()
    {
        try
        {
            Day d = (Day) accessor.getInstance(Day.class);
            Assert.assertNull(d);

        }
        catch (PropertyAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }
}
