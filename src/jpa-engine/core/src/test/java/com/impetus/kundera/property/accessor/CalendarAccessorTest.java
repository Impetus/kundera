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

import java.util.Calendar;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 * 
 */
public class CalendarAccessorTest
{
    PropertyAccessor<Calendar> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new CalendarAccessor();

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
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(Calendar.class, null));
        
        Calendar v1 = Calendar.getInstance();
        byte[] b = accessor.toBytes(v1);
        
        Calendar v2 = accessor.fromBytes(Calendar.class, b);
        Assert.assertEquals(v1, v2);        
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        Calendar v1 = Calendar.getInstance();
        byte[] b = accessor.toBytes(v1);
        
        Calendar v2 = accessor.fromBytes(Calendar.class, b);
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));
        Calendar v1 = Calendar.getInstance();
        String s1 = v1.getTime().getTime() + "";
        
        String s2 = accessor.toString(v1);
        Assert.assertTrue(s1.equals(s2));       
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Calendar.class, null));

        Calendar v1 = Calendar.getInstance();

        String s = v1.getTime().getTime() + "";
        Calendar v2 = accessor.fromString(Calendar.class, s);
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#getCopy(java.lang.Object)}
     * .
     */
    @Test
    public void testGetCopy()
    {
        Assert.assertNull(accessor.getCopy(null));

        Calendar v1 = Calendar.getInstance();
        Calendar v2 = accessor.getCopy(v1);

        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CalendarAccessor#getInstance(java.lang.Class)}
     * .
     */
    @Test
    public void testGetInstance()
    {
        Assert.assertNotNull(accessor.getInstance(Calendar.class));
        Assert.assertTrue(accessor.getInstance(Calendar.class) instanceof Calendar);
    }

}
