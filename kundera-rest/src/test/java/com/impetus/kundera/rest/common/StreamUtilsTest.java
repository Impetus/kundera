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

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

/**
 * Test case for {@link StreamUtils} 
 * @author amresh.singh
 */
public class StreamUtilsTest extends TestCase
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
     * Test method for {@link com.impetus.kundera.rest.common.StreamUtils#toString(java.io.InputStream)}.
     */
    public void testToStringInputStream()
    {
        try
        {
            String str = "<book><isbn>34523423423423</isbn><author>Amresh</author><publication>Willey</publication></book>";
            InputStream is = StreamUtils.toInputStream(str);
            String str2 = StreamUtils.toString(is);
            assertNotNull(str2);
            assertEquals(str2, str);
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }
    }

    /**
     * Test method for {@link com.impetus.kundera.rest.common.StreamUtils#toInputStream(java.lang.String)}.
     */
    public void testToInputStream()
    {
        String str = "<book><isbn>34523423423423</isbn><author>Amresh</author><publication>Willey</publication></book>";
        InputStream is = StreamUtils.toInputStream(str);
        assertNotNull(is);
    }

}
