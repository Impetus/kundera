/**
 * 
 */
package com.impetus.kundera.property.accessor;

import java.sql.Time;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author impadmin
 * 
 */
public class SQLTimeAccessorTest
{

    private Time t = new Time(12345678L);

    private SQLTimeAccessor accessor;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        
        accessor = new SQLTimeAccessor();
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
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#fromBytes(java.lang.Class, byte[])}
     * .
     */
    @Test
    public void testFromBytes()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.property.accessor.SQLTimeAccessor#toBytes(java.lang.Object)}
     * .
     */
    @Test
    public void testToBytes()
    {
        Time t1 = accessor.fromBytes(Time.class, accessor.toBytes(t));
        Assert.assertEquals(t.getTime(), t1.getTime());
    }
}
