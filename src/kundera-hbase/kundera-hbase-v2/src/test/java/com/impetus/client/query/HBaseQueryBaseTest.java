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

import java.util.Collections;
import java.util.List;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class HBaseQueryBaseTest.
 * 
 * @author Devender Yadav
 */
public class HBaseQueryBaseTest extends BookBaseTest
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
        testSelectWithInClause();
        testSelectFields();
    }

    /**
     * Test delete queries.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testDeleteQueries() throws Exception
    {
        testDeleteAll();
        testDeleteOnId();
        testDeleteWithWhereClause();
        testDeleteWithInClause();
    }

    /**
     * Test update queries.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testUpdateQueries() throws Exception
    {
        testUpdateAll();
        testUpdateOnId();
        testUpdateWithWhereClause();
        testUpdateWithInClause();
    }

    /**
     * Test select all.
     */
    private void testSelectAll()
    {
        List<Book> results = em.createQuery("select b from Book b").getResultList();
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
        List<Book> results = em.createQuery("select b from Book b where b.bookId=1").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook1(results.get(0));

        results = em.createQuery("select b from Book b where b.bookId <> 1").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, F, T, T, T);

        results = em.createQuery("Select b from Book b where b.bookId < 3").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, T, T, F, F);

        results = em.createQuery("Select b from Book b where b.bookId <= 3").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, T, F);

        results = em.createQuery("Select b from Book b where b.bookId > 2").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, F, T, T);

        results = em.createQuery("Select b from Book b where b.bookId >= 2").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, F, T, T, T);

        results = em.createQuery("Select b from Book b where  b.bookId >= 1 and b.bookId < 3").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, T, T, F, F);

        results = em.createQuery("Select b from Book b where  b.bookId > 1 and b.bookId <= 3").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, T, T, F);

        results = em.createQuery("select b from Book b where b.bookId in (3,4,5,6)").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, F, T, T);

    }

    /**
     * Test select with where clause.
     */
    private void testSelectWithWhereClause()
    {
        /*------queries with where clause------*/
        List<Book> results = em.createQuery("select b from Book b where b.title = 'book1'").getResultList();
        Assert.assertNotNull(results);
        validateBook1(results.get(0));

        results = em.createQuery("select b from Book b where b.author = 'author2'").getResultList();
        Assert.assertNotNull(results);
        validateBook2(results.get(0));

        results = em.createQuery("select b from Book b where b.author = 'author1'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, T, F, F, T);

        /*------queries with where clause and comparison operators------*/
        results = em.createQuery("select b from Book b where b.year > 2000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, F, T, T, T);

        results = em.createQuery("select b from Book b where b.year >= 2000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        assertResults(results, T, T, T, T);

        results = em.createQuery("select b from Book b where b.year < 2015").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, T, F);

        results = em.createQuery("select b from Book b where b.year <= 2015").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        assertResults(results, T, T, T, T);

        results = em.createQuery("select b from Book b where b.year = 2005").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook2(results.get(0));

        results = em.createQuery("select b from Book b where b.year <> 2010").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, F, T);

        /*------queries with where clause, comparison operators and logical operators------*/
        results = em.createQuery("select b from Book b where b.author = 'author1' and b.pages > 200").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook4(results.get(0));

        results = em.createQuery("select b from Book b where b.year > 2000 and b.pages > 200").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, F, T, T);

        results = em.createQuery("select b from Book b where b.year >= 2000 and b.pages < 500").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        assertResults(results, T, T, T, T);

        // not working with lucene
        results = em.createQuery("select b from Book b where b.year > 2010 or b.year <= 2005").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, F, T);

        results = em.createQuery("select b from Book b where b.year < 2010 or b.pages >= 350").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, F, T);

        /*
         * This type of query is not working with Lucene as indexing store
         * meanwhile you can use it directly with HBase.
         */

        // results = em.createQuery(
        // "select b from Book b where b.year > 2010 and b.pages > 300 or b.year <= 2005 and b.pages > 100")
        // .getResultList();
        // Assert.assertNotNull(results);
        // Assert.assertEquals(2, results.size());
        // assertResults(results, F, T, F, T);

        results = em
                .createQuery("select b from Book b where b.author = 'author1' and b.year <> 2015 and b.pages < 300")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validateBook1(results.get(0));

        results = em.createQuery("select b from Book b where b.pages = 100 or b.year <= 2005 or b.title = 'book4'")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, T, T, F, T);

    }

    /**
     * Test select with in clause.
     */
    private void testSelectWithInClause()
    {
        List<Book> results = em.createQuery("select b from Book b where b.title in ('book1','book2')").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, T, T, F, F);

        results = em.createQuery("select b from Book b where b.year in (2005,2010)").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, F, T, T, F);

    }

    /**
     * Test select fields.
     */
    private void testSelectFields()
    {
        List results = em.createQuery("select b.title from Book b").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Collections.sort(results);
        Assert.assertEquals("book1", results.get(0));
        Assert.assertEquals("book2", results.get(1));
        Assert.assertEquals("book3", results.get(2));
        Assert.assertEquals("book4", results.get(3));

        results = em.createQuery("select b.author from Book b where b.bookId=1").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("author1", results.get(0));

        results = em.createQuery("select b.pages from Book b where b.year > 2000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Collections.sort(results);
        Assert.assertEquals(200, results.get(0));
        Assert.assertEquals(300, results.get(1));
        Assert.assertEquals(400, results.get(2));

        results = em.createQuery("select b.title, b.year from Book b where b.year <> 2010").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        for (Object result : results)
        {
            if ("book1".equals(((List) result).get(0)))
            {
                Assert.assertEquals(2000, ((List) result).get(1));
            }
            else if ("book2".equals(((List) result).get(0)))
            {

                Assert.assertEquals(2005, ((List) result).get(1));
            }
            else if ("book3".equals(((List) result).get(0)))
            {
                Assert.assertEquals(2015, ((List) result).get(1));
            }
        }

        results = em.createQuery("select b.title , b.pages from Book b where b.author = 'author1' and b.pages > 200")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("book4", ((List) results.get(0)).get(0));
        Assert.assertEquals(400, ((List) results.get(0)).get(1));

        //TODO ..check this
        results = em.createQuery("select b.bookId , b.title from Book b where b.author = 'author1' and b.pages > 200")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(4, ((List) results.get(0)).get(0));
        Assert.assertEquals("book4", ((List) results.get(0)).get(1));

        results = em
                .createQuery(
                        "select b.author, b.pages, b.year from Book b where b.author = 'author1' and b.year <> 2015 and b.pages < 300")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("author1", ((List) results.get(0)).get(0));
        Assert.assertEquals(100, ((List) results.get(0)).get(1));
        Assert.assertEquals(2000, ((List) results.get(0)).get(2));

        results = em.createQuery("select b.title from Book b where b.bookId in (3,4,5,6)").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Collections.sort(results);
        Assert.assertEquals("book3", results.get(0));
        Assert.assertEquals("book4", results.get(1));

        results = em.createQuery("select b.author,b.pages from Book b where b.title in ('book1','book5','book6')")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("author1", ((List) results.get(0)).get(0));
        Assert.assertEquals(100, ((List) results.get(0)).get(1));
    }

    /**
     * Test delete all.
     */
    private void testDeleteAll()
    {
        persistBooks();
        int result = em.createQuery("delete from Book b").executeUpdate();
        Assert.assertEquals(4, result);
        assertDeleted(T, T, T, T);
    }

    /**
     * Test delete on id.
     */
    private void testDeleteOnId()
    {
        persistBooks();
        int result = em.createQuery("delete from Book b where b.bookId = 1").executeUpdate();
        Assert.assertEquals(1, result);
        assertDeleted(T, F, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId <> 1").executeUpdate();
        Assert.assertEquals(3, result);
        assertDeleted(F, T, T, T);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId < 3").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(T, T, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId <= 3").executeUpdate();
        Assert.assertEquals(3, result);
        assertDeleted(T, T, T, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId > 2").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(F, F, T, T);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId >= 2").executeUpdate();
        Assert.assertEquals(3, result);
        assertDeleted(F, T, T, T);

        persistBooks();
        result = em.createQuery("delete from Book b where  b.bookId >= 1 and b.bookId < 3").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(T, T, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where  b.bookId > 1 and b.bookId <= 3").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(F, T, T, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.bookId in (2,3,4,5)").executeUpdate();
        Assert.assertEquals(3, result);
        assertDeleted(F, T, T, T);

    }

    /**
     * Test delete with where clause.
     */
    private void testDeleteWithWhereClause()
    {
        persistBooks();
        int result = em.createQuery("delete from Book b where b.title = 'book1'").executeUpdate();
        Assert.assertEquals(1, result);
        assertDeleted(T, F, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.author = 'author1'").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(T, F, F, T);

        persistBooks();
        result = em.createQuery("delete from Book b where b.year <= 2015").executeUpdate();
        Assert.assertNotNull(result);
        Assert.assertEquals(4, result);
        assertDeleted(T, T, T, T);

        persistBooks();
        result = em.createQuery("delete from Book b where b.year = 2005").executeUpdate();
        Assert.assertEquals(1, result);
        assertDeleted(F, T, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.year <> 2010").executeUpdate();
        Assert.assertNotNull(result);
        Assert.assertEquals(3, result);
        assertDeleted(T, T, F, T);

        persistBooks();
        result = em.createQuery("delete from Book b where b.author = 'author1' and b.year <> 2015 and b.pages < 300")
                .executeUpdate();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result);
        assertDeleted(T, F, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.pages = 100 or b.year <= 2005 or b.title = 'book4'")
                .executeUpdate();
        Assert.assertNotNull(result);
        Assert.assertEquals(3, result);
        assertDeleted(T, T, F, T);

    }

    /**
     * Test delete with in clause.
     */
    private void testDeleteWithInClause()
    {
        persistBooks();
        int result = em.createQuery("delete from Book b where b.title in ('book1','book2')").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(T, T, F, F);

        persistBooks();
        result = em.createQuery("delete from Book b where b.year in (2005,2010)").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(F, T, T, F);

    }

    /**
     * Test update all.
     */
    private void testUpdateAll()
    {
        persistBooks();
        int result = em.createQuery("update Book b set b.title = 'book'").executeUpdate();
        Assert.assertEquals(4, result);
        em.clear();
        Book book1 = em.find(Book.class, 1);
        Book book2 = em.find(Book.class, 2);
        Book book3 = em.find(Book.class, 3);
        Book book4 = em.find(Book.class, 4);
        Assert.assertEquals("book", book1.getTitle());
        Assert.assertEquals("book", book2.getTitle());
        Assert.assertEquals("book", book3.getTitle());
        Assert.assertEquals("book", book4.getTitle());

        persistBooks();
        result = em.createQuery("update Book b set b.pages = 300").executeUpdate();
        Assert.assertEquals(4, result);
        em.clear();
        book1 = em.find(Book.class, 1);
        book2 = em.find(Book.class, 2);
        book3 = em.find(Book.class, 3);
        book4 = em.find(Book.class, 4);
        Assert.assertEquals(300, book1.getPages());
        Assert.assertEquals(300, book2.getPages());
        Assert.assertEquals(300, book3.getPages());
        Assert.assertEquals(300, book4.getPages());
    }

    /**
     * Test update on id.
     */
    private void testUpdateOnId()
    {
        persistBooks();
        int result = em.createQuery("update Book b set b.year = 2009 where b.bookId = 1").executeUpdate();
        Assert.assertEquals(1, result);
        em.clear();
        Book book1 = em.find(Book.class, 1);
        Assert.assertEquals(2009, book1.getYear());
        Assert.assertEquals("book1", book1.getTitle());

        persistBooks();
        result = em.createQuery("update Book b set b.year = 2009 where b.bookId <> 1").executeUpdate();
        Assert.assertEquals(3, result);
        em.clear();
        Book book2 = em.find(Book.class, 2);
        Assert.assertEquals(2009, book2.getYear());
        Assert.assertEquals("book2", book2.getTitle());

        Book book3 = em.find(Book.class, 3);
        Assert.assertEquals(2009, book3.getYear());
        Assert.assertEquals("book3", book3.getTitle());

        Book book4 = em.find(Book.class, 4);
        Assert.assertEquals(2009, book4.getYear());
        Assert.assertEquals("book4", book4.getTitle());

        persistBooks();
        result = em.createQuery("update Book b set b.pages = 500 where b.bookId < 3").executeUpdate();
        Assert.assertEquals(2, result);
        em.clear();
        book1 = em.find(Book.class, 1);
        Assert.assertEquals(500, book1.getPages());
        Assert.assertEquals("book1", book1.getTitle());
        book2 = em.find(Book.class, 2);
        Assert.assertEquals(500, book2.getPages());
        Assert.assertEquals("book2", book2.getTitle());

        persistBooks();
        result = em.createQuery("update Book b set b.author = 'author5' where b.bookId in (2,3,5)").executeUpdate();
        Assert.assertEquals(2, result);
        em.clear();
        book2 = em.find(Book.class, 2);
        book3 = em.find(Book.class, 3);
        Assert.assertEquals("author5", book2.getAuthor());
        Assert.assertEquals("author5", book3.getAuthor());
    }

    /**
     * Test update with where clause.
     */
    private void testUpdateWithWhereClause()
    {
        persistBooks();
        int result = em.createQuery("update Book b set b.year = 2009 where b.title = 'book1'").executeUpdate();
        Assert.assertEquals(1, result);
        em.clear();
        Book book1 = em.find(Book.class, 1);
        Assert.assertEquals(2009, book1.getYear());
        Assert.assertEquals("book1", book1.getTitle());

        persistBooks();
        result = em.createQuery("update Book b set b.year = 2009,b.title='book' where b.title = 'book1'")
                .executeUpdate();
        Assert.assertEquals(1, result);
        em.clear();
        book1 = em.find(Book.class, 1);
        Assert.assertEquals(2009, book1.getYear());
        Assert.assertEquals("book", book1.getTitle());

        persistBooks();
        result = em
                .createQuery(
                        "update Book b set b.author = 'author', b.pages = 500 where b.author = 'author1' and b.year <> 2015 and b.pages < 300")
                .executeUpdate();
        Assert.assertEquals(1, result);
        em.clear();
        book1 = em.find(Book.class, 1);
        Assert.assertEquals("author", book1.getAuthor());
        Assert.assertEquals(500, book1.getPages());
    }

    /**
     * Test update with in clause.
     */
    private void testUpdateWithInClause()
    {
        persistBooks();
        int result = em.createQuery("update Book b set b.author = 'author5' where b.title in ('book1','book2')")
                .executeUpdate();
        Assert.assertEquals(2, result);
        em.clear();
        Book book1 = em.find(Book.class, 1);
        Book book2 = em.find(Book.class, 2);
        Assert.assertEquals("author5", book1.getAuthor());
        Assert.assertEquals("author5", book2.getAuthor());

        persistBooks();
        result = em.createQuery("update Book b set b.title = 'book', b.year = 2000 where b.year in (2005,2010)")
                .executeUpdate();

        Assert.assertEquals(2, result);

        em.clear();
        book1 = em.find(Book.class, 2);
        book2 = em.find(Book.class, 3);
        Assert.assertEquals("book", book1.getTitle());
        Assert.assertEquals("book", book2.getTitle());
        Assert.assertEquals(2000, book1.getYear());
        Assert.assertEquals(2000, book2.getYear());

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
