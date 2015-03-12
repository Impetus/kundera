/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.Query;

/**
 * @author vivek.mishra junit for {@link IResultIterator}.
 */
public class ResultIteratorTest extends BookBaseTest
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
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
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
        em = emf.createEntityManager();
        persistBooks();
    }

    @Test
    public void scrollTest() throws Exception
    {
        assertBegin();
    }

    private void assertBegin()
    {
        String query = "Select b from Book b";
        List<Book> results = assertBookScrolling(query, 4);
        assertResults(results, T, T, T, T);
        assertForFetchSize(query, 4);
        
        query = "select b from Book b where b.title = 'book1'";
        results = assertBookScrolling(query, 1);
        assertResults(results, T, F, F, F);
        assertForFetchSize(query, 1);
        
        query = "select b from Book b where b.author = 'author2'";
        results = assertBookScrolling(query, 1);
        assertResults(results, F, T, F, F);
        
        query = "select b from Book b where b.author = 'author1'";
        results = assertBookScrolling(query, 2);
        assertResults(results, T, F, F, T);
        
        query = "select b from Book b where b.year > 2000";
        results = assertBookScrolling(query, 3);
        assertResults(results, F, T, T, T);
        assertForFetchSize(query, 3);
        
        query = "select b from Book b where b.year >= 2000";
        results = assertBookScrolling(query, 4);
        assertResults(results, T, T, T, T);
        assertForFetchSize(query, 4);
        
        query = "select b from Book b where b.year < 2015";
        results = assertBookScrolling(query, 3);
        assertResults(results, T, T, T, F);
        assertForFetchSize(query, 3);

    }

    private List<Book> assertBookScrolling(String queryClause, int expected)
    {
        Query query = (Query) em.createQuery(queryClause, Book.class);

        int count = 0;
        Iterator<Book> bookItr = query.iterate();
        List<Book> books = new ArrayList<Book>();
        while (bookItr.hasNext())
        {
            Book book = bookItr.next();
            Assert.assertNotNull(book);
            count++;
            books.add(book);
        }
        Assert.assertTrue(count > 0);
        Assert.assertTrue(count == expected);
        return books;
    }

    private void assertForFetchSize(final String qry, int expectedCount)
    {
        Query query = (Query) em.createQuery(qry, Book.class);

        assertOnFetch(query, 0, expectedCount);
        assertOnFetch(query, 1, expectedCount);
        assertOnFetch(query, 2, expectedCount); 
        assertOnFetch(query, 3, expectedCount);
        assertOnFetch(query, 4, expectedCount);
        assertOnFetch(query, null, expectedCount);

    }

    private void assertOnFetch(Query query, Integer fetchSize, int available)
    {
        query.setFetchSize(fetchSize);
        int counter = 0;
        Iterator<Book> iter = query.iterate();

        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            counter++;
        }

        Assert.assertEquals(counter, fetchSize == null || available < fetchSize ? available : fetchSize);
        try
        {
            iter.next();
            Assert.fail();
        }
        catch (NoSuchElementException nsex)
        {
            Assert.assertNotNull(nsex.getMessage());
        }
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
        em.close();
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
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }
}