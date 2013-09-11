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
import java.io.InputStream;
import java.io.IOException;


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
      try
       {
    	File file = new File(".");
    	iterator = new ClassFileIterator(file, filter);  
    	int c ;
       	int count = 0;
    	int countFiles = 0;
    	InputStream fis;
    	while ((fis = iterator.next()) != null){
    		countFiles ++;
    		/*while((c = fis.read()) != -1){
        		count++;
              	 
        	}*/
    		Assert.assertNotNull(fis);
    		Assert.assertNotNull(fis.available());
        	Assert.assertNotNull(count);
        	Assert.assertEquals(fis.getClass(),FileInputStream.class);
        	
        	fis.close();
    	}
    	
    	Assert.assertEquals(countFiles,iterator.getSize());
    	
    	
       }
       catch(IOException ioe)
       {
    	   Assert.fail(ioe.getMessage());
       }
    	
    }

    
}
