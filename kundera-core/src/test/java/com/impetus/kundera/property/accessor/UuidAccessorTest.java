/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.impetus.kundera.property.accessor;

import com.impetus.kundera.property.PropertyAccessException;
import java.util.UUID;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author kcarlson
 */
public class UuidAccessorTest
{
    
    public UuidAccessorTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
    
    @Test
    public void testUuidAccessor() throws PropertyAccessException
    {
        UUID uuid = UUID.randomUUID();
        
        UuidAccessor uuidAccessor = new UuidAccessor();
        
        byte[] bytes = uuidAccessor.toBytes(uuid);
        
        String str = uuidAccessor.toString(uuid);
        
        assert uuid.toString().equals(str);
        
        UUID fromBytes = uuidAccessor.fromBytes(bytes);
        
        assert uuid.equals(fromBytes);
        
        UUID fromString = uuidAccessor.fromString(str);
        
        assert uuid.equals(fromString);
    }
}
