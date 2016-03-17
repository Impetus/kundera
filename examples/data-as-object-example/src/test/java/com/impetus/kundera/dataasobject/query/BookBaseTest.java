/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.query;

import java.util.List;

import com.impetus.kundera.dataasobject.entities.Book;

import junit.framework.Assert;

/**
 * The Class BookBaseTest.
 * 
 * @author Pragalbh Garg
 */
public abstract class BookBaseTest
{

    /** The t. */
    protected boolean T = true;

    /** The f. */
    protected boolean F = false;

    /**
     * Validate book1.
     * 
     * @param book
     *            the book
     */
    protected void validateBook1(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(Integer.valueOf(1), book.getBookId());
        Assert.assertEquals("book1", book.getTitle());
        Assert.assertEquals("author1", book.getAuthor());
        Assert.assertEquals(2000, book.getYear());
        Assert.assertEquals(100, book.getPages());
    }

    /**
     * Validate book2.
     * 
     * @param book
     *            the book
     */
    protected void validateBook2(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(Integer.valueOf(2), book.getBookId());
        Assert.assertEquals("book2", book.getTitle());
        Assert.assertEquals("author2", book.getAuthor());
        Assert.assertEquals(2005, book.getYear());
        Assert.assertEquals(200, book.getPages());
    }

    /**
     * Validate book3.
     * 
     * @param book
     *            the book
     */
    protected void validateBook3(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(Integer.valueOf(3), book.getBookId());
        Assert.assertEquals("book3", book.getTitle());
        Assert.assertEquals("author3", book.getAuthor());
        Assert.assertEquals(2010, book.getYear());
        Assert.assertEquals(300, book.getPages());
    }

    /**
     * Validate book4.
     * 
     * @param book
     *            the book
     */
    protected void validateBook4(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(Integer.valueOf(4), book.getBookId());
        Assert.assertEquals("book4", book.getTitle());
        Assert.assertEquals("author1", book.getAuthor());
        Assert.assertEquals(2015, book.getYear());
        Assert.assertEquals(400, book.getPages());
    }

    /**
     * Assert results.
     * 
     * @param results
     *            the results
     * @param foundBook1
     *            the found book1
     * @param foundBook2
     *            the found book2
     * @param foundBook3
     *            the found book3
     * @param foundBook4
     *            the found book4
     */
    protected void assertResults(List<Book> results, boolean foundBook1, boolean foundBook2, boolean foundBook3,
            boolean foundBook4)
    {
        for (Book book : (List<Book>) results)
        {
            switch (book.getBookId())
            {
            case 1:
                if (foundBook1)
                    validateBook1(book);
                else
                    Assert.fail();
                break;
            case 2:
                if (foundBook2)
                    validateBook2(book);
                else
                    Assert.fail();
                break;
            case 3:
                if (foundBook3)
                    validateBook3(book);
                else
                    Assert.fail();
                break;
            case 4:
                if (foundBook4)
                    validateBook4(book);
                else
                    Assert.fail();
                break;
            }
        }
    }

    /**
     * Assert deleted.
     * 
     * @param foundBook1
     *            the b1
     * @param foundBook2
     *            the b2
     * @param foundBook3
     *            the b3
     * @param foundBook4
     *            the b4
     */
    protected void assertDeleted(Boolean foundBook1, Boolean foundBook2, Boolean foundBook3, Boolean foundBook4)
    {
        Book book1 = new Book().find(1);
        Book book2 = new Book().find(2);
        Book book3 = new Book().find(3);
        Book book4 = new Book().find(4);
        if (foundBook1)
        {
            Assert.assertNull(book1);
        }
        else
        {
            Assert.assertNotNull(book1);
        }
        if (foundBook2)
        {
            Assert.assertNull(book2);
        }
        else
        {
            Assert.assertNotNull(book2);
        }
        if (foundBook3)
        {
            Assert.assertNull(book3);
        }
        else
        {
            Assert.assertNotNull(book3);
        }
        if (foundBook4)
        {
            Assert.assertNull(book4);
        }
        else
        {
            Assert.assertNotNull(book4);
        }
    }
}
