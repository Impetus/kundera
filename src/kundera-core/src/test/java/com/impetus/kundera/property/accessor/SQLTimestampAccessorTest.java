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

import java.sql.Timestamp;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 *
 */
public class SQLTimestampAccessorTest
{
    PropertyAccessor<Timestamp> accessor;
    

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new SQLTimestampAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(Timestamp.class, null));
        
        long l = System.currentTimeMillis();
        Timestamp t = new Timestamp(l);
        byte[] b = accessor.toBytes(t);
        
        Timestamp t2 = accessor.fromBytes(Timestamp.class, b);
        Assert.assertEquals(t, t2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        
        long l = System.currentTimeMillis();
        Timestamp t = new Timestamp(l);
        byte[] b = accessor.toBytes(t);
        
        Timestamp t2 = accessor.fromBytes(Timestamp.class, b);
        Assert.assertEquals(t, t2);        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));
        
        long l = System.currentTimeMillis();
        Timestamp t = new Timestamp(l);        
        Assert.assertEquals("" + l, accessor.toString(t));      
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Timestamp.class, null));        
        long currTime = System.currentTimeMillis();
        Timestamp t = accessor.fromString(Timestamp.class, currTime + "");
        Assert.assertEquals(currTime, t.getTime());
        
        String s = t.toString();
        Timestamp t2 = accessor.fromString(Timestamp.class, s);
        Assert.assertEquals(currTime, t2.getTime());        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        Assert.assertNull(accessor.getCopy(null));
        
        long currTime = System.currentTimeMillis();
        Timestamp t1 = new Timestamp(currTime);
        Timestamp t2 = accessor.getCopy(t1);
        Assert.assertNotNull(t2);
        Assert.assertEquals(currTime, t2.getTime());        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLTimestampAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Timestamp.class);
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof Timestamp);
        Assert.assertEquals(Integer.MAX_VALUE, ((Timestamp) o).getTime());
    }

}
