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
package com.impetus.kundera.classreading;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author amresh.singh
 *
 */
public class ClasspathReaderTest
{
    ClasspathReader reader;
    Filter filter;
    

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
        reader = null;
        filter = null;
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.ClasspathReader#findResources()}.
     */
    @Test
    public void testFindResources()
    {
        reader = new ClasspathReader();
        URL[] urls = reader.findResources();
        Assert.assertNull(urls);
        
        List<String> classesToScan = new ArrayList<String>();
        classesToScan.add("com.impetus.kundera.PersonnelDTO");
        classesToScan.add("com.impetus.kundera.query.Person");
        reader = new ClasspathReader(classesToScan);
        urls = reader.findResources();
        Assert.assertNotNull(urls);
        Assert.assertTrue(urls.length > 0);    
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.ClasspathReader#findResourcesAsStream()}.
     */
    @Test
    public void testFindResourcesAsStream()
    {
        //When classes to scan is null
        reader = new ClasspathReader();
        InputStream[] streams = reader.findResourcesAsStream();
        Assert.assertNull(streams);
        
        List<String> classesToScan = new ArrayList<String>();
        classesToScan.add("com.impetus.kundera.PersonnelDTO");
        classesToScan.add("com.impetus.kundera.query.Person");
        reader = new ClasspathReader(classesToScan);
        streams = reader.findResourcesAsStream();
       reader.read();
        Assert.assertNotNull(streams);
        Assert.assertTrue(streams.length > 0); 
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.ClasspathReader#findResourcesByClasspath()}.
     */
    @Test
    public void testFindResourcesByClasspath()
    {
        reader = new ClasspathReader();
        URL[] urls = reader.findResourcesByClasspath();
        Assert.assertNotNull(urls);
        Assert.assertTrue(urls.length > 0);     
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.ClasspathReader#setFilter(com.impetus.kundera.classreading.Filter)}.
     */
    @Test
    public void testSetFilter()
    {
        reader = new ClasspathReader();
        reader.setFilter(filter);
        Assert.assertNotNull(reader.getFilter());
    }

}
