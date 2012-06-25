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

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Test case for {@link JAXBUtils}
 * 
 * @author amresh.singh
 */
public class JAXBUtilsTest extends TestCase
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
     * {@link com.impetus.kundera.rest.common.JAXBUtils#toObject(java.lang.String, java.lang.Class)}
     * .
     */
    public void testXMLToObject()
    {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><book><isbn>34523423423423</isbn><author>Amresh</author><publication>Willey</publication></book>";
        try
        {

            Book book = (Book) JAXBUtils
                    .toObject(StreamUtils.toInputStream(xml), Book.class, MediaType.APPLICATION_XML);
            assertNotNull(book);
            assertEquals("34523423423423", book.getIsbn());
            assertEquals("Amresh", book.getAuthor());
            assertEquals("Willey", book.getPublication());
        }
        catch (Exception e)
        {
            fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.rest.common.JAXBUtils#toObject(java.lang.String, java.lang.Class)}
     * .
     */
    public void testJSONToObject()
    {

        String json = "{book:{\"isbn\":\"2222\",\"author\":\"Kuldeep\", \"publication\":\"McGraw\"}}";
        try
        {

            Book book = (Book) JAXBUtils.toObject(StreamUtils.toInputStream(json), Book.class,
                    MediaType.APPLICATION_JSON);
            assertNotNull(book);
            assertEquals("2222", book.getIsbn());
            assertEquals("Kuldeep", book.getAuthor());
            assertEquals("McGraw", book.getPublication());
        }
        catch (Exception e)
        {
            fail(e.getMessage());
        }
    }
    
    public void testObjectToXML() {
        Book b = new Book();
        b.setIsbn("11111111111");
        b.setAuthor("Xamry");
        b.setPublication("McGraw");
        
        
        String s = JAXBUtils.toString(Book.class, b, MediaType.APPLICATION_XML);
        Assert.assertNotNull(s);
        
    }

}
