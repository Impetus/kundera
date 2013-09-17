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

import java.util.UUID;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.property.PropertyAccessor;

/**
 * @author amresh.singh
 *
 */
public class UUIDAccessorTest
{
    PropertyAccessor<UUID> accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new UUIDAccessor();
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
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    @Test
    public void testFromBytes()
    {
        Assert.assertNull(accessor.fromBytes(UUID.class, null));
        UUID uuid = UUID.randomUUID();
        byte[] b = accessor.toBytes(uuid);
        
        UUID uuid2 = accessor.fromBytes(UUID.class, b);
        Assert.assertEquals(uuid, uuid2);

    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        Assert.assertNull(accessor.toBytes(null));
        UUID uuid = UUID.randomUUID();
        byte[] b = accessor.toBytes(uuid);
        
        UUID uuid2 = accessor.fromBytes(UUID.class, b);
        Assert.assertEquals(uuid, uuid2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#toString(java.lang.Object)}.
     */
    @Test
    public void testToStringObject()
    {
        Assert.assertNull(accessor.toString(null));
        UUID uuid = UUID.randomUUID();
        String s1 = uuid.toString();
        String s2 = accessor.toString(uuid);
        Assert.assertTrue(s1.equals(s2));
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    @Test
    public void testFromString()
    {
        Assert.assertNull(accessor.fromString(UUID.class, null));
        
        UUID uuid = UUID.randomUUID();
        String s = uuid.toString();
        
        UUID uuid2 = accessor.fromString(UUID.class, s);
        Assert.assertEquals(uuid, uuid2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#getCopy(java.lang.Object)}.
     */
    @Test
    public void testGetCopy()
    {
        UUID uuid = UUID.randomUUID();
        
        UUID uuid2 = accessor.getCopy(uuid);
        Assert.assertEquals(uuid, uuid2);

    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.UUIDAccessor#getInstance(java.lang.Class)}.
     */
    @Test
    public void testGetInstance()
    {
        Object o = accessor.getInstance(UUID.class);
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof UUID);
    }

}
