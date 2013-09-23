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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author amresh.singh
 *
 */
public class FilterTest
{
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
        filter = null;
    }

    /**
     * Test method for {@link com.impetus.kundera.classreading.Filter#accepts(java.lang.String)}.
     */
    @Test
    public void testAccepts()
    {
        String fileName = "/com/impetus/kundera/entities/User.class";
        Assert.assertTrue(filter.accepts(fileName));
        
        fileName = "javax/persistence/EntityManager.class";
        Assert.assertFalse(filter.accepts(fileName));
        
    }

}
