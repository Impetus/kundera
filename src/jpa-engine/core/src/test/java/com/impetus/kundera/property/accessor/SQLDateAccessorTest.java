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

import java.sql.Date;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 *
 */
public class SQLDateAccessorTest
{
    PropertyAccessor<Date> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new SQLDateAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(Date.class, null));
        
        long l = System.currentTimeMillis();        
        Date d = new Date(l);
        byte[] b = accessor.toBytes(d);
        
        Date d2 = accessor.fromBytes(Date.class, b);
        
        Assert.assertEquals(d, d2);       
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        
        long l = System.currentTimeMillis();        
        Date d = new Date(l);       
        byte[] b = accessor.toBytes(d);
        
        Date d2 = accessor.fromBytes(Date.class, b);
        
        Assert.assertEquals(d, d2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));
        
        long l = System.currentTimeMillis();        
        Date d = new Date(l);        
        
        Assert.assertEquals(String.valueOf(d.getTime()), accessor.toString(d));
        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(Date.class, null));
        
        long l = System.currentTimeMillis();        
        Date d = new Date(l);        
        Date d2 = accessor.fromString(Date.class, "" + l);
        Assert.assertEquals(d, d2);
        
        Assert.assertEquals(d.getYear(), accessor.fromString(Date.class, d2.toString()).getYear());
        Assert.assertEquals(d.getMonth(), accessor.fromString(Date.class, d2.toString()).getMonth());
        Assert.assertEquals(d.getDate(), accessor.fromString(Date.class, d2.toString()).getDate());        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        long l = System.currentTimeMillis();
        Date d = new Date(l);
        
        Date d2 = accessor.getCopy(d);
        Assert.assertNotNull(d2);
        Assert.assertTrue(d.equals(d2));        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.SQLDateAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(Date.class);
        Assert.assertNotNull(o);
        
        Assert.assertEquals(Integer.MAX_VALUE, ((Date) o).getTime());        
    }

}
