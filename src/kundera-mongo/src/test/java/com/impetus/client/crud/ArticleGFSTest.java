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
package com.impetus.client.crud;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.crud.entities.ArticleGFS;
import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.KunderaException;

/**
 * The Class ArticleGFSTest.
 */
public class ArticleGFSTest
{

    /** The Constant _PU. */
    private static final String _PU = "mongoTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
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
    }

    @Test
    public void testComplexQueries()
    {
        prepareArticle("article1", date("2017-01-05 10:00"), date("2017-01-05 10:00"), "First article", "important", 0,
                true);
        prepareArticle("article2", date("2017-01-15 10:00"), date("2017-01-15 10:00"), "Second article", null, 0, true);
        prepareArticle("article3", date("2017-01-25 10:00"), date("2017-01-25 10:00"), "Third article", "important", 0,
                true);
        prepareArticle("article4", date("2017-01-30 10:00"), null, "Fourth article", "important", 0, true);
        prepareArticle("article5", date("2017-02-05 10:00"), date("2017-02-55 10:00"), "Fifth article", null, 0, true);

        Query query = em.createQuery("select a from ArticleGFS a where a.show = :show and "
                + "(a.displayDate is null or a.displayDate < :date1 or a.displayDate > :date2) "
                + "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        List<ArticleGFS> results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Assert.assertEquals("First article", results.get(0).getTitle());
        Assert.assertEquals("Second article", results.get(1).getTitle());
        Assert.assertEquals("Fourth article", results.get(2).getTitle());
        Assert.assertEquals("Fifth article", results.get(3).getTitle());

        query = em.createQuery("select a from ArticleGFS a where a.show = :show and "
                + "(a.displayDate is null or a.displayDate < :date1 or a.displayDate > :date2 and a.category is null) "
                + "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Assert.assertEquals("First article", results.get(0).getTitle());
        Assert.assertEquals("Second article", results.get(1).getTitle());
        Assert.assertEquals("Fourth article", results.get(2).getTitle());
        Assert.assertEquals("Fifth article", results.get(3).getTitle());

        query = em.createQuery("select a from ArticleGFS a where a.show = :show and "
                + "(a.displayDate > :date2 and a.category is null or a.displayDate is null or a.displayDate < :date1) "
                + "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Assert.assertEquals("First article", results.get(0).getTitle());
        Assert.assertEquals("Second article", results.get(1).getTitle());
        Assert.assertEquals("Fourth article", results.get(2).getTitle());
        Assert.assertEquals("Fifth article", results.get(3).getTitle());

        query = em
                .createQuery("select a from ArticleGFS a where a.show = :show and "
                        + "(a.category is null and (a.displayDate > :date2 or a.displayDate is null or a.displayDate < :date1)) "
                        + "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("Second article", results.get(0).getTitle());
        Assert.assertEquals("Fifth article", results.get(1).getTitle());

        query = em.createQuery("select a from ArticleGFS a where a.show = :show and "
                + "(a.createDate < :date and (a.displayDate is null or a.displayDate < :date)) "
                + "and a.category = :category " + "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date", date("2017-03-30 00:00"));
        query.setParameter("category", "important");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals("First article", results.get(0).getTitle());
        Assert.assertEquals("Third article", results.get(1).getTitle());
        Assert.assertEquals("Fourth article", results.get(2).getTitle());
    }

    @Test(expected = Exception.class)
    public void testAggregation()
    {
        prepareArticle("article1", date("2017-01-05 10:00"), null, "First article", "A", 1, true);
        prepareArticle("article2", date("2017-01-15 10:00"), null, "Second article", "B", 3, true);
        prepareArticle("article3", date("2017-01-25 10:00"), null, "Third article", "A", 6, true);

        Query query = em.createQuery("select max(a.priority) from ArticleGFS a");
        List<?> results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(6, results.get(0));

        query = em.createQuery("select max(a.priority), min(a.priority) from ArticleGFS a");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Object[] resultArray = (Object[]) results.get(0);
        Assert.assertEquals("Results: " + Arrays.toString(resultArray), 2, resultArray.length);
        Assert.assertEquals(6, resultArray[0]);
        Assert.assertEquals(1, resultArray[1]);

        query = em
                .createQuery("select avg(a.priority), sum(a.priority), max(a.priority), min(a.priority) from ArticleGFS a");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        resultArray = (Object[]) results.get(0);
        Assert.assertEquals("Results: " + Arrays.toString(resultArray), 4, resultArray.length);
        assertAlmostEqual(3.3333, resultArray[0]);
        Assert.assertEquals(10, resultArray[1]);
        Assert.assertEquals(6, resultArray[2]);
        Assert.assertEquals(1, resultArray[3]);

        query = em
                .createQuery("select sum(a.priority), a.category from ArticleGFS a group by a.category order by a.category");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("A", ((Object[]) results.get(0))[1]);
        Assert.assertEquals("B", ((Object[]) results.get(1))[1]);

        for (Object item : results)
        {
            resultArray = (Object[]) item;
            Assert.assertEquals("Result Item: " + Arrays.toString(resultArray), 2, resultArray.length);
            Assert.assertTrue(Arrays.asList("A", "B").contains(resultArray[1]));

            if (resultArray[1].equals("A"))
            {
                Assert.assertEquals(7, resultArray[0]);
            }
            else if (resultArray[1].equals("B"))
            {
                Assert.assertEquals(3, resultArray[0]);
            }
        }

        query = em
                .createQuery("select sum(a.priority), a.category, avg(a.priority), min(a.priority) from ArticleGFS a group by a.category");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertSumsMatch(results, 4);

        query = em
                .createQuery("select sum(a.priority), a.category from ArticleGFS a group by a.category order by sum(a.priority) desc");
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("A", ((Object[]) results.get(0))[1]);
        Assert.assertEquals("B", ((Object[]) results.get(1))[1]);
        assertSumsMatch(results, 2);
    }

    @Test(expected = Exception.class)
    public void testCountAggregation()
    {
        prepareArticle("article1", date("2017-01-05 10:00"), null, "First article", "A", 1, true);
        prepareArticle("article2", date("2017-01-15 10:00"), null, "Second article", "B", 3, true);
        prepareArticle("article3", date("2017-01-25 10:00"), null, "Third article", "A", 6, true);

        Query query = em
                .createQuery("select sum(a.priority), count(a), a.category from ArticleGFS a group by a.category order by a.category");
        List results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(2L, ((Object[]) results.get(0))[1]);
        Assert.assertEquals(1L, ((Object[]) results.get(1))[1]);

        query = em.createQuery("select count(a) from ArticleGFS a");
        Object result = query.getSingleResult();

        Assert.assertNotNull(result);
        Assert.assertEquals(3L, result);
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
        em.close();
    }

    /**
     * @throws Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        MongoUtils.dropDatabase(emf, _PU);
        emf.close();
    }

    private void assertAlmostEqual(double expected, Object actual)
    {
        double value = (double) actual;
        Assert.assertTrue(
                String.format("%.4f != %.4f\nExpected: %.4f\nActual  : %.4f", expected, value, expected, value),
                Math.abs(value - expected) < 0.0001);
    }

    private void assertSumsMatch(List results, int expectedNumberOfItems)
    {
        for (Object item : results)
        {
            Object[] resultArray = (Object[]) item;
            Assert.assertEquals("Result Item: " + Arrays.toString(resultArray), expectedNumberOfItems,
                    resultArray.length);
            Assert.assertTrue(Arrays.asList("A", "B").contains(resultArray[1]));

            if (resultArray[1].equals("A"))
            {
                Assert.assertEquals(7, resultArray[0]);
            }
            else if (resultArray[1].equals("B"))
            {
                Assert.assertEquals(3, resultArray[0]);
            }
        }
    }

    private void prepareArticle(String id, Date createDate, Date displayDate, String title, String category,
            int priority, boolean show)
    {
        ArticleGFS item = new ArticleGFS();
        item.setArticleId(id);
        item.setCreateDate(createDate);
        item.setDisplayDate(displayDate);
        item.setTitle(title);
        item.setCategory(category);
        item.setPriority(priority);
        item.setShow(show);
        item.setData("TestData".getBytes());
        em.persist(item);
    }

    private static Date date(String value)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try
        {
            return format.parse(value);
        }
        catch (ParseException ex)
        {
            throw new AssertionError("Invalid date: " + value, ex);
        }
    }

}
