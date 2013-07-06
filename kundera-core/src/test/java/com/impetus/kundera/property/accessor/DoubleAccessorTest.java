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
public class DoubleAccessorTest
{
    PropertyAccessor<Double> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new DoubleAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertEquals(0.0, accessor.fromBytes(Double.class, null));
        
        Double d1 = new Double(4.555);
        byte[] b = accessor.toBytes(d1);
        
        Double d2 = accessor.fromBytes(Double.class, b);
        
        Assert.assertEquals(d1, d2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));

        Double d1 = new Double(4.555);
        
        byte[] b = accessor.toBytes(d1);
        
        Double d2 = accessor.fromBytes(Double.class, b);
        
        Assert.assertEquals(d1, d2);

    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));

        Double d1 = new Double(4.555);
        String s1 = d1.toString();
        
        String s2 = accessor.toString(d1);
        
        Assert.assertTrue(s1.equals(s2));   
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Double.class, null));
        Double d1 = new Double(4.555);
        String s = d1.toString();
        
        Double d2 = accessor.fromString(Double.class, s);
        Assert.assertEquals(d1, d2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        Double d1 = new Double(4.555);
        Double d2 = accessor.getCopy(d1);
        Assert.assertEquals(d1, d2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.DoubleAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Double.class);
        Assert.assertNotNull(o);
        Assert.assertEquals(Double.MAX_VALUE, (Double) o);

    }

}
