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
public class FloatAccessorTest
{
    PropertyAccessor<Float> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new FloatAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertEquals(0.0f, accessor.fromBytes(Float.class, null));
        
        Float f1 = new Float(4.555);
        byte[] b = accessor.toBytes(f1);
        
        Float f2 = accessor.fromBytes(Float.class, b);
        
        Assert.assertEquals(f1, f2);
        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));

        Float f1 = new Float(4.555);        
        byte[] b = accessor.toBytes(f1);        
        Float f2 = accessor.fromBytes(Float.class, b);
        
        Assert.assertEquals(f1, f2);        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));

        Float f1 = new Float(4.555);
        String s1 = f1.toString();
        
        String s2 = accessor.toString(f1);
        
        Assert.assertTrue(s1.equals(s2)); 
        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Float.class, null));
        Float f1 = new Float(4.555);
        String s = f1.toString();
        
        Float f2 = accessor.fromString(Float.class, s);
        Assert.assertEquals(f1, f2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        Float f1 = new Float(4.555);
        Float f2 = accessor.getCopy(f1);
        Assert.assertEquals(f1, f2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.FloatAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Float.class);
        Assert.assertNotNull(o);
        Assert.assertEquals(Float.MAX_VALUE, (Float) o);
    }

}
