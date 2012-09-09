/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera;

import javax.persistence.spi.LoadState;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link PersistenceUtilHelper}
 * @author amresh.singh
 */
public class PersistenceUtilHelperTest
{
    private final PersistenceUtilHelper.MetadataCache cache = new PersistenceUtilHelper.MetadataCache();
    
    public static class FieldAccessBean  {
        public String publicAccessProperty;
        protected String protectedAccessProperty;
        private String privateAccessProperty;
    }
 

    public static class MethodAccessBean {
        private String publicAccessProperty;
        private String protectedAccessProperty;
        private String privateAccessProperty;
        
        public String getPublicAccessPropertyValue() {
            return publicAccessProperty;
        }

        protected String getProtectedAccessPropertyValue() {
            return protectedAccessProperty;
        }

        private String getPrivateAccessPropertyValue() {
            return privateAccessProperty;
        }
    }   

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
     * Test method for {@link com.impetus.kundera.PersistenceUtilHelper#isLoadedWithoutReference(java.lang.Object, java.lang.String, com.impetus.kundera.PersistenceUtilHelper.MetadataCache)}.
     */
    @Test
    public void testIsLoadedWithoutReference()
    {
        
    }

    /**
     * Test method for {@link com.impetus.kundera.PersistenceUtilHelper#isLoadedWithReference(java.lang.Object, java.lang.String, com.impetus.kundera.PersistenceUtilHelper.MetadataCache)}.
     */
    @Test
    public void testIsLoadedWithReference()
    {
        // Public field
        Assert.assertEquals(LoadState.UNKNOWN,
                PersistenceUtilHelper.isLoadedWithReference(new FieldAccessBean(), "publicAccessProperty", cache));

        // Public method
        Assert.assertEquals(LoadState.UNKNOWN,
                PersistenceUtilHelper.isLoadedWithReference(new MethodAccessBean(), "publicAccessPropertyValue", cache));

        // Protected field
        Assert.assertEquals(LoadState.UNKNOWN,
                PersistenceUtilHelper.isLoadedWithReference(new FieldAccessBean(), "protectedAccessProperty", cache));

        // Protected method
        Assert.assertEquals(LoadState.UNKNOWN, 
                PersistenceUtilHelper.isLoadedWithReference(new MethodAccessBean(), "protectedAccessPropertyValue", cache));

        // Private field
        Assert.assertEquals(LoadState.UNKNOWN,
                PersistenceUtilHelper.isLoadedWithReference(new FieldAccessBean(), "privateAccessProperty", cache));

        // Private method
        Assert.assertEquals(LoadState.UNKNOWN, PersistenceUtilHelper.isLoadedWithReference(new MethodAccessBean(),
                "privateAccessPropertyValue", cache));
        
    }

    /**
     * Test method for {@link com.impetus.kundera.PersistenceUtilHelper#isLoaded(java.lang.Object)}.
     */
    @Test
    public void testIsLoaded()
    {
        
    }
}
