/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.classreading;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;




/**
 * @author chhavi.gangwal
 * junit for {@link ClassFileIterator}
 */
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
    public void testIterateWithFilter()
    {
    	file = new File(".");
    	
    	//ClassFilterIterator with filter
    	iterator = new ClassFileIterator(file, filter);  
    	Assert.assertEquals(iterator.next().getClass(),FileInputStream.class);
    	Assert.assertNotNull(iterator.next());
      }
      
      /**
       * Test method for {@link com.impetus.kundera.classreading.ClassFileIterator#next()}.
       */
      @Test
      public void testIterateWithoutFilter()
      {
      	
        try {
      	
      	   file = new File(".");
      	   //ClassFilterIterator without filter
      	   iterator = new ClassFileIterator(file);  
      	   Assert.assertEquals(iterator.next().getClass(),FileInputStream.class);
      	   Assert.assertNotNull(iterator.next());
      	
      }
        catch (ResourceReadingException e)
        {
      	  
      	  Assert.assertEquals("Couldn't read file .",e.getMessage());
         }
         
      }
      
      
      /**
       * Test method for {@link com.impetus.kundera.classreading.ClassFileIterator#next()}.
       */
      @Test
      public void testNoClassIterate()
      {
      	
        try {
      	     	    	
      	  //ClassFilterIterator no file found
      	   file = new File("/com/impetus/kundera/");
      	   iterator = new ClassFileIterator(file);
      	}
        catch (ResourceReadingException e)
        {
          Assert.assertNull(iterator.next());
      	  Assert.assertEquals("Couldn't read file /com/impetus/kundera",e.getMessage());
         }
         
      }
       
    	
    

    
}
