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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.core.QueryType;
import com.impetus.kundera.dataasobject.entities.Book;

import junit.framework.Assert;

/**
 * The Class CassandraQueryTest.
 * 
 * @author Devender Yadav
 */
public class CassandraQueryTest extends BookBaseTest
{

    /**
     * Sets the up before class.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Book.bind("client-properties.json", Book.class);
    }

    /**
     * Sets the up.
     *
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        persistBooks();
    }

    /**
     * Test select queries.
     *
     * @throws Exception
     *             the exception
     */
    @Test
    public void testSelectQueries() throws Exception
    {
        testSelectAll();
        testSelectOnId();
        testSelectWithWhereClause();
        testSelectFields();
    }

    /**
     * Test native queries.
     */
    @Test
    public void testNativeQueries()
    {
        List results = new Book().query("select \"TITLE\" from \"Book\"", QueryType.NATIVE);
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());

        results = new Book().query("select \"AUTHOR\" from \"Book\" where token(\"ID\") = token(1)", QueryType.NATIVE);
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("author1", ((Book) results.get(0)).getAuthor());
    }

    /**
     * Test select all.
     */
    private void testSelectAll()
    {
        List<Book> results = new Book().query("select b from Book b");
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Assert.assertTrue(Book.class.isAssignableFrom(results.get(0).getClass()));
        assertResults(results, T, T, T, T);
    }

    /**
     * Test select on id.
     */
    private void testSelectOnId()
    {
        List<Book> results = new Book().query("select b from Book b where b.bookId=1");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook1(results.get(0));

        results = new Book().query("select b from Book b where b.bookId in (3,4,5,6)");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, F, T, T);

    }

    /**
     * Test select with where clause.
     */
    private void testSelectWithWhereClause()
    {
        List<Book> results = new Book().query("select b from Book b where b.title = 'book1'");
        Assert.assertNotNull(results);
        validateBook1(results.get(0));

        results = new Book().query("select b from Book b where b.author = 'author2'");
        Assert.assertNotNull(results);
        validateBook2(results.get(0));

        results = new Book().query("select b from Book b where b.year = 2005");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook2(results.get(0));

    }

    /**
     * Test select fields.
     */
    private void testSelectFields()
    {
        List results = new Book().query("select b.title from Book b");
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());

        results = new Book().query("select b.author from Book b where b.bookId=1");
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("author1", ((Book) results.get(0)).getAuthor());

        results = new Book().query("select b.title from Book b where b.bookId in (3,4,5,6)");
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
    }

    /**
     * Tear down.
     *
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        deleteBooks();
    }

    /**
     * Tear down after class.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Book.unbind();
    }

    /**
     * Persist books.
     */
    private void persistBooks()
    {
        Book book1 = prepareData(1, "book1", "author1", 2000, 100);
        Book book2 = prepareData(2, "book2", "author2", 2005, 200);
        Book book3 = prepareData(3, "book3", "author3", 2010, 300);
        Book book4 = prepareData(4, "book4", "author1", 2015, 400);
        book1.save();
        book2.save();
        book3.save();
        book4.save();
    }

    /**
     * Delete books.
     */
    private void deleteBooks()
    {
        Book b1 = new Book().find(1);
        b1.delete();
        Book b2 = new Book().find(2);
        b2.delete();

        Book b3 = new Book().find(3);
        b3.delete();

        Book b4 = new Book().find(4);
        b4.delete();
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
    private Book prepareData(int bookId, String title, String author, int year, int pages)
    {
        Book book = new Book();
        book.setBookId(bookId);
        book.setTitle(title);
        book.setAuthor(author);
        book.setYear(year);
        book.setPages(pages);
        return book;
    }

}
