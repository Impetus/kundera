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
public class ObjectAccessorTest
{
    
    private PropertyAccessor<Object> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new ObjectAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(PersonalDetail.class, null));
        PersonalDetail pd = new PersonalDetail("Amresh", "password", "single");
        byte[] b = accessor.toBytes(pd);
        
        Object o = accessor.fromBytes(PersonalDetail.class, b);
        
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof PersonalDetail);
        Assert.assertTrue(o.equals(pd));
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        PersonalDetail pd = new PersonalDetail("Amresh", "password", "single");
        byte[] b = accessor.toBytes(pd);
        Object o = accessor.fromBytes(PersonalDetail.class, b);
        Assert.assertNotNull(o);
        Assert.assertTrue(pd.equals(o));

    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        PersonalDetail pd = new PersonalDetail("Amresh", "password", "single");  
        String s = accessor.toString(pd);
        Assert.assertNotNull(s);

    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(String.class, null));
        
        Object o = accessor.fromString(String.class, "Hello");
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof String);
        Assert.assertEquals("Hello", (String)o);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        Assert.assertNull(accessor.getCopy(null));
        
        PersonalDetail pd = new PersonalDetail("Amresh", "password", "single");       
        
        Object pdCopy = accessor.getCopy(pd);
        Assert.assertNotNull(pdCopy);
        Assert.assertTrue(pdCopy instanceof PersonalDetail);
        Assert.assertTrue(pd == pdCopy);
        Assert.assertTrue(pd.getPersonalDetailId() == ((PersonalDetail)pdCopy).getPersonalDetailId());
        Assert.assertTrue(pd.getName() == ((PersonalDetail)pdCopy).getName());
        Assert.assertTrue(pd.getPassword() == ((PersonalDetail)pdCopy).getPassword());
        Assert.assertTrue(pd.getRelationshipStatus() == ((PersonalDetail)pdCopy).getRelationshipStatus());
        
        byte[] b = "Hello".getBytes();
        Object bCopy = accessor.getCopy(b);
        Assert.assertNotNull(bCopy);
        Assert.assertTrue(bCopy instanceof byte[]);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ObjectAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(PersonalDetail.class);
        Assert.assertNotNull(o);
        
    }

}
