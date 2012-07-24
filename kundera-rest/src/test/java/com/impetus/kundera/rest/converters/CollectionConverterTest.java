/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.rest.converters;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.rest.common.Book;

/**
 * @author amresh
 * 
 */
public class CollectionConverterTest
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
     * {@link com.impetus.kundera.rest.converters.CollectionConverter#toString(java.util.Collection, java.lang.Class, java.lang.String)}
     * .
     */
    @Test
    public void testToStringCollectionOfQClassOfQString()
    {
        List books = new ArrayList();

        Book book1 = new Book();
        book1.setIsbn("11111111111");
        book1.setAuthor("Amresh");
        book1.setPublication("AAA");

        Book book2 = new Book();
        book2.setIsbn("22222222222");
        book2.setAuthor("Vivek");
        book2.setPublication("BBB");

        books.add(book1);
        books.add(book2);

        String s = CollectionConverter.toString(books, Book.class, MediaType.APPLICATION_XML);
        Assert.assertNotNull(s);
    }

    @Test
    public void testToCollection()
    {

        String s = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><books><book><author>Saurabh</author><isbn>1111111111111</isbn><publication>Willey</publication></book><book><author>Vivek</author><isbn>2222222222222</isbn><publication>OReilly</publication></book><books>";
        Collection c = CollectionConverter.toCollection(s, ArrayList.class, Book.class, MediaType.APPLICATION_XML);
        Assert.assertNotNull(c);
        Assert.assertFalse(c.isEmpty());
        Assert.assertEquals(2, c.size());

    }

}
