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
public class BooleanAccessorTest
{
    PropertyAccessor<Boolean> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new BooleanAccessor();
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
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertFalse(accessor.fromBytes(Boolean.class, null));
        
        byte[] b = accessor.toBytes(new Boolean(true));
        
        Boolean bb = accessor.fromBytes(Boolean.class, b);
        Assert.assertEquals(true, bb.booleanValue());
        
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        byte[] b = accessor.toBytes(new Boolean(true));
        
        Boolean bb = accessor.fromBytes(Boolean.class, b);
        Assert.assertEquals(true, bb.booleanValue());
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#toString(java.lang.Object)}
     * .
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));

        Assert.assertEquals("true", accessor.toString(new Boolean(true)));
        Assert.assertEquals("false", accessor.toString(new Boolean(false)));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Boolean.class, null));

        Assert.assertTrue(accessor.fromString(Boolean.class, "true"));
        Assert.assertFalse(accessor.fromString(Boolean.class, "false"));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#getCopy(java.lang.Object)}
     * .
     */
    @Test
    public void testGetCopy()
    {
        Assert.assertNull(accessor.getCopy(null));

        Assert.assertTrue(accessor.getCopy(new Boolean(true)));
        Assert.assertFalse(accessor.getCopy(new Boolean(false)));

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.BooleanAccessor#getInstance(java.lang.Class)}
     * .
     */
    @Test
    public void testGetInstance()
    {
        Assert.assertTrue((Boolean) accessor.getInstance(Boolean.class));
    }

}
