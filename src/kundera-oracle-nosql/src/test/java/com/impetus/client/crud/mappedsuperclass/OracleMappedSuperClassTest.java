package com.impetus.client.crud.mappedsuperclass;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;

public class OracleMappedSuperClassTest extends MappedSuperClassBase
{

    @Before
    public void setUp() throws Exception
    {
        _PU = "twikvstore";
        setUpInternal();
    }
    
    @Test
    public void test()
    {
        assertInternal(true);
    }

    
    @After
    public void tearDown() throws Exception
    {
        tearDownInternal();
        

    }
}
