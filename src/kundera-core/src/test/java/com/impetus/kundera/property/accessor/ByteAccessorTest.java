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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 *
 */
public class ByteAccessorTest
{
    PropertyAccessor<Byte> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new ByteAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(Byte.class, null));
        Byte v1 = new Byte("7");
        byte[] b = accessor.toBytes(v1);
        
        Byte v2 = accessor.fromBytes(Byte.class, b);
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        Byte v1 = new Byte("7");
        byte[] b = accessor.toBytes(v1);
        
        Byte v2 = accessor.fromBytes(Byte.class, b);
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));
        Byte v1 = new Byte("7");
        
        String s = accessor.toString(v1);
        
        Assert.assertEquals("7", s);
        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Byte.class, null));
        
        Byte v1 = new Byte("7");
        Byte v2 = accessor.fromString(Byte.class, "7");
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        Assert.assertNull(accessor.getCopy(null));
        
        Byte v1 = new Byte("7");
        Byte v2 = accessor.getCopy(v1);
        Assert.assertEquals(v1, v2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ByteAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Assert.assertNotNull(accessor.getInstance(Byte.class));
        Assert.assertTrue(accessor.getInstance(Byte.class) instanceof Byte);
        Assert.assertEquals(Byte.MAX_VALUE, accessor.getInstance(Byte.class));        
    }

}
