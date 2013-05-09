/**
 * 
 */
package com.impetus.kundera.property.accessor;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author impadmin
 * 
 */
public class CharAccessorTest
{

    CharAccessor accessor = new CharAccessor();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CharAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {
        byte[] b = { 0, 65 };
        char a = accessor.fromBytes(Character.class, b);
        Assert.assertEquals('A', a);
    }


    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.CharAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testFromString()
    {
        String a = "A";
        char c = accessor.fromString(Character.class, a);
        Assert.assertEquals('A',c);
    }

}
