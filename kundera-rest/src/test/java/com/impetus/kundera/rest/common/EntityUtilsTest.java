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
package com.impetus.kundera.rest.common;

import javax.ws.rs.HttpMethod;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class EntityUtilsTest
{

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
     * Test method for
     * {@link com.impetus.kundera.rest.common.EntityUtils#isValidQuery(java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testIsValidQuery()
    {
        /** Positive scenario */
        String insertQuery = "Insert into Book values (1, Amresh)";
        Assert.assertTrue(EntityUtils.isValidQuery(insertQuery, HttpMethod.POST));

        String selectQuery = "Select b from Book b";
        Assert.assertTrue(EntityUtils.isValidQuery(selectQuery, HttpMethod.GET));

        String updateQuery = "update Book set author=Amresh";
        Assert.assertTrue(EntityUtils.isValidQuery(updateQuery, HttpMethod.PUT));

        String deleteQuery = "delete from Book";
        Assert.assertTrue(EntityUtils.isValidQuery(deleteQuery, HttpMethod.DELETE));

        /** Negative scenario - Unmatched Http Method */
        String insertQuery2 = "Insert into Book values (1, Amresh)";
        Assert.assertFalse(EntityUtils.isValidQuery(insertQuery2, HttpMethod.GET));

        String selectQuery2 = "Select b from Book b";
        Assert.assertFalse(EntityUtils.isValidQuery(selectQuery2, HttpMethod.POST));

        String updateQuery2 = "update Book set author=Amresh";
        Assert.assertFalse(EntityUtils.isValidQuery(updateQuery2, HttpMethod.DELETE));

        String deleteQuery2 = "delete from Book";
        Assert.assertFalse(EntityUtils.isValidQuery(deleteQuery2, HttpMethod.PUT));

        /** Negative scenario - Junk Values */
        String junkQuery = "Blah Blah Blah";
        Assert.assertFalse(EntityUtils.isValidQuery(junkQuery, HttpMethod.GET));
        Assert.assertFalse(EntityUtils.isValidQuery(junkQuery, HttpMethod.POST));
        Assert.assertFalse(EntityUtils.isValidQuery(junkQuery, HttpMethod.DELETE));
        Assert.assertFalse(EntityUtils.isValidQuery(junkQuery, HttpMethod.PUT));
    }

}
