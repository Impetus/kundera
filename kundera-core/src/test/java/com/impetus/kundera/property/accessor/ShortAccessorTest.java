/**
 * 
 */
package com.impetus.kundera.property.accessor;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author impadmin
 *
 */
public class ShortAccessorTest
{
    
    ShortAccessor accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        accessor = new ShortAccessor();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ShortAccessor#fromBytes(java.lang.Class, byte[])}.
     */
    //@Test
    public void testFromBytes()
    {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ShortAccessor#toBytes(java.lang.Object)}.
     */
    @Test
    public void testToBytes()
    {
        short s = -1;
        byte[] b = accessor.toBytes(s);
        short s2 = accessor.fromBytes(Short.class, b);
        Assert.assertEquals(s, s2);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ShortAccessor#toString(java.lang.Object)}.
     */
    //@Test
    public void testToStringObject()
    {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.impetus.kundera.property.accessor.ShortAccessor#fromString(java.lang.Class, java.lang.String)}.
     */
    //@Test
    public void testFromString()
    {
        fail("Not yet implemented");
    }

}
