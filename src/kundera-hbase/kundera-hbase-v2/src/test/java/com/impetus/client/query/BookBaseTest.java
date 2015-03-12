/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.query;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import junit.framework.Assert;

/**
 * The Class BookBaseTest.
 * 
 * @author Pragalbh Garg
 */
public class BookBaseTest
{

    /** The Constant SCHEMA. */
    protected static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    protected static final String HBASE_PU = "queryTest";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected EntityManager em;

    /** The t. */
    protected boolean T = true;

    /** The f. */
    protected boolean F = false;

    /**
     * Persist books.
     */
    protected void persistBooks()
    {
        Book book1 = prepareData(1, "book1", "author1", 2000, 100);
        Book book2 = prepareData(2, "book2", "author2", 2005, 200);
        Book book3 = prepareData(3, "book3", "author3", 2010, 300);
        Book book4 = prepareData(4, "book4", "author1", 2015, 400);
        em.persist(book1);
        em.persist(book2);
        em.persist(book3);
        em.persist(book4);
        em.clear();
    }

    /**
     * Delete books.
     */
    protected void deleteBooks()
    {
        em.createQuery("delete from Book b").executeUpdate();
        em.clear();
    }

    /**
     * Prepare data.
     * 
     * @param bookId
     *            the book id
     * @param title
     *            the title
     * @param author
     *            the author
     * @param year
     *            the year
     * @param pages
     *            the pages
     * @return the book
     */
    protected Book prepareData(int bookId, String title, String author, int year, int pages)
    {
        Book book = new Book();
        book.setBookId(bookId);
        book.setTitle(title);
        book.setAuthor(author);
        book.setYear(year);
        book.setPages(pages);
        return book;
    }

    /**
     * Validate book1.
     * 
     * @param book
     *            the book
     */
    protected void validateBook1(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(1, book.getBookId());
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
        Assert.assertEquals(2, book.getBookId());
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
        Assert.assertEquals(3, book.getBookId());
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
        Assert.assertEquals(4, book.getBookId());
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
                    Assert.assertTrue(false);
                break;
            case 2:
                if (foundBook2)
                    validateBook2(book);
                else
                    Assert.assertTrue(false);
                break;
            case 3:
                if (foundBook3)
                    validateBook3(book);
                else
                    Assert.assertTrue(false);
                break;
            case 4:
                if (foundBook4)
                    validateBook4(book);
                else
                    Assert.assertTrue(false);
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
        Book book1 = em.find(Book.class, 1);
        Book book2 = em.find(Book.class, 2);
        Book book3 = em.find(Book.class, 3);
        Book book4 = em.find(Book.class, 4);
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
