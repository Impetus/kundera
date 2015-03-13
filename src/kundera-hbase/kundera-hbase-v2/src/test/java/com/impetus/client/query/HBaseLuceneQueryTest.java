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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * The Class HBaseLuceneQueryTest.
 * 
 * @author Devender Yadav
 */
public class HBaseLuceneQueryTest extends HBaseQueryBaseTest
{

    /** The property map. */
    private static Map<String, String> propertyMap = new HashMap<String, String>();

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void setUpBeforeClass()
    {
        propertyMap.put("index.home.dir", "lucene");
        emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.query.HBaseQueryBaseTest#tearDown()
     */
    @After
    public void tearDown() throws Exception
    {
        deleteBooks();
        LuceneCleanupUtilities.cleanDir("lucene");
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

    /**
     * Lucene test.
     */
    @Test
    public void luceneTest()
    {
        likeQueryTest();
    }

    /**
     * Like query test.
     */
    private void likeQueryTest()
    {
        Book b1 = prepareData(11, "TO THE LIGHTHOUSE", "Virginia Woolf", 1927, 100);
        Book b2 = prepareData(12, "The House Of Mirth", "Edith Wharton", 1905, 200);
        Book b3 = prepareData(13, "under the volcano", "Malcolm Lowry", 1947, 300);
        em.persist(b1);
        em.persist(b2);
        em.persist(b3);
        em.clear();

        String qry = "Select b from Book b where b.title like :title";
        Query q = em.createQuery(qry);
        q.setParameter("title", "volcano");
        List<Book> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertBook3(results.get(0));

        qry = "Select b from Book b where b.title like :title";
        q = em.createQuery(qry);
        q.setParameter("title", "House");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        for (Book book : results)
        {
            if (book.getBookId() == 11)
            {
                assertBook1(book);
            }

            else if (book.getBookId() == 12)
            {
                assertBook2(book);
            }
        }

        qry = "Select b from Book b where b.title like :title";
        q = em.createQuery(qry);
        q.setParameter("title", "%der");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertBook3(results.get(0));

        qry = "Select b from Book b where b.title like :title";
        q = em.createQuery(qry);
        q.setParameter("title", "und%");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertBook3(results.get(0));

        qry = "Select b from Book b where b.author like :author";
        q = em.createQuery(qry);
        q.setParameter("author", "Lowry");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertBook3(results.get(0));

        qry = "Select b from Book b where b.author like :author";
        q = em.createQuery(qry);
        q.setParameter("author", "%hart%");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertBook2(results.get(0));
    }

    /**
     * Assert book1.
     * 
     * @param book
     *            the book
     */
    private void assertBook1(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(11, book.getBookId());
        Assert.assertEquals("TO THE LIGHTHOUSE", book.getTitle());
        Assert.assertEquals("Virginia Woolf", book.getAuthor());
        Assert.assertEquals(1927, book.getYear());
        Assert.assertEquals(100, book.getPages());
    }

    /**
     * Assert book2.
     * 
     * @param book
     *            the book
     */
    private void assertBook2(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(12, book.getBookId());
        Assert.assertEquals("The House Of Mirth", book.getTitle());
        Assert.assertEquals("Edith Wharton", book.getAuthor());
        Assert.assertEquals(1905, book.getYear());
        Assert.assertEquals(200, book.getPages());
    }

    /**
     * Assert book3.
     * 
     * @param book
     *            the book
     */
    private void assertBook3(Book book)
    {
        Assert.assertNotNull(book);
        Assert.assertEquals(13, book.getBookId());
        Assert.assertEquals("under the volcano", book.getTitle());
        Assert.assertEquals("Malcolm Lowry", book.getAuthor());
        Assert.assertEquals(1947, book.getYear());
        Assert.assertEquals(300, book.getPages());
    }

}
