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
        accessor = null;
    }

    @Test
    public void testFromBytes()
    {
        Assert.assertEquals(new Short((short)0), accessor.fromBytes(Short.class, null));
        
        Short d1 = new Short((short)4);
        byte[] b = accessor.toBytes(d1);
        
        Short d2 = accessor.fromBytes(Short.class, b);
        
        Assert.assertEquals(d1, d2);
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.ShortAccessor#toBytes(java.lang.Object)}
     * .
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
     * Test method for
     * {@link com.impetus.kundera.property.accessor.ShortAccessor#toString(java.lang.Object)}
     * .
     */
     @Test
    public void testToStringObject()
    {
         Assert.assertNull(accessor.toString(null));

         Short d1 = new Short((short)4);
         String s1 = d1.toString();
         
         String s2 = accessor.toString(d1);
         
         Assert.assertTrue(s1.equals(s2));  
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.ShortAccessor#fromString(java.lang.Class, java.lang.String)}
     * .
     */
     @Test
    public void testFromString()
    {
         Assert.assertNull(accessor.fromString(Short.class, null));
         Short d1 = new Short((short)4);
         String s = d1.toString();
         
         Short d2 = accessor.fromString(Short.class, s);
         Assert.assertEquals(d1, d2);
    }
     
     @Test
     public void testGetCopy()
     {
         Short d1 = new Short((short)4);
         Short d2 = accessor.getCopy(d1);
         Assert.assertEquals(d1, d2);
     }


     @Test
     public void testGetInstance()
     {
         Object o = accessor.getInstance(Short.class);
         Assert.assertNotNull(o);        
     }

}
