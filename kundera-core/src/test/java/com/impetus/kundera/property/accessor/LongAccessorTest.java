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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author vivek.mishra
 * 
 */
public class LongAccessorTest
{

    private PropertyAccessor<Long> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new LongAccessor();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testFromBytes()
    {
        byte[] bytes = new byte[] {};
        Assert.assertNull(accessor.fromBytes(LongAccessor.class, bytes));
        Long l = 49L;
        bytes = accessor.toBytes(l);
        Assert.assertEquals(l, accessor.fromBytes(LongAccessor.class, bytes));

        l = 12l;
        bytes = accessor.toBytes(l);
        Assert.assertEquals(l, (Long) accessor.fromBytes(LongAccessor.class, bytes));

    }

    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));

        Long d1 = new Long(4);

        byte[] b = accessor.toBytes(d1);

        Long d2 = accessor.fromBytes(Long.class, b);

        Assert.assertEquals(d1, d2);

    }

    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));

        Long d1 = new Long(4);
        String s1 = d1.toString();

        String s2 = accessor.toString(d1);

        Assert.assertTrue(s1.equals(s2));
    }

    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Long.class, null));
        Long d1 = new Long(4);
        String s = d1.toString();

        Long d2 = accessor.fromString(Long.class, s);
        Assert.assertEquals(d1, d2);
    }

    @Test
    public void testGetCopy()
    {
        Long d1 = new Long(4);
        Long d2 = accessor.getCopy(d1);
        Assert.assertEquals(d1, d2);
    }

    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Long.class);
        Assert.assertNotNull(o);
    }

}
