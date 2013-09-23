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

import junit.framework.TestCase;

/**
 * Test case for {@link TokenUtils}
 * 
 * @author amresh.singh
 */
public class TokenUtilsTest extends TestCase
{

    protected void setUp() throws Exception
    {
        super.setUp();

    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.rest.common.TokenUtils#generateApplicationToken()}
     * .
     */
    public void testGenerateApplicationToken()
    {
        String applicationToken = TokenUtils.generateApplicationToken();
        assertNotNull(applicationToken);
        assertTrue(applicationToken.startsWith(Constants.APPLICATION_TOKEN_PREFIX + "_"));
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.rest.common.TokenUtils#generateSessionToken()}
     * .
     */
    public void testGenerateSessionToken()
    {
        String sessionToken = TokenUtils.generateSessionToken();
        assertNotNull(sessionToken);
        assertTrue(sessionToken.startsWith(Constants.SESSION_TOKEN_PREFIX + "_"));
    }

}
