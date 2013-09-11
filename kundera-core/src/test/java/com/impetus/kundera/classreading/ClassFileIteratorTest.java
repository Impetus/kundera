package com.impetus.kundera.classreading;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;



import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClassFileIteratorTest {
	ClassFileIterator iterator;
    Filter filter;
    File file;
    

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    	iterator = new ClassFileIterator(file);  
        filter = new FilterImpl();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    	iterator = null;
        filter = null;
        file =  null;
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.ClassFileIterator#next()}.
     */
    @Test
    public void testaddFilesToIterate()
    {
    	
    	File file = new File(".");
    	iterator = new ClassFileIterator(file, filter);  
    	InputStream fis = iterator.next();
    	Assert.assertNotNull(fis);
    	Assert.assertEquals(fis.getClass(),FileInputStream.class);
    	
    }

    
}
