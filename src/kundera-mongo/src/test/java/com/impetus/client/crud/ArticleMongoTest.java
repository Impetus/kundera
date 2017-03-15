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

import com.impetus.client.crud.entities.ArticleMongo;
import com.impetus.client.utils.MongoUtils;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * The Class ArticleMongoTest.
 */
public class ArticleMongoTest
{

    /** The Constant _PU. */
    private static final String _PU = "mongoTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
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
        MongoUtils.dropDatabase(emf, _PU);
        emf.close();
    }

    @Test
    public void testComplexQueries() {
        prepareArticle("article1", date("2017-01-05 10:00"), date("2017-01-05 10:00"), "First article", "important", 0, true);
        prepareArticle("article2", date("2017-01-15 10:00"), date("2017-01-15 10:00"), "Second article", null, 0, true);
        prepareArticle("article3", date("2017-01-25 10:00"), date("2017-01-25 10:00"), "Third article", "important", 0, true);
        prepareArticle("article4", date("2017-01-30 10:00"), null, "Fourth article", "important", 0, true);
        prepareArticle("article5", date("2017-02-05 10:00"), date("2017-02-55 10:00"), "Fifth article", null, 0, true);

        Query query = em.createQuery(
              "select a from ArticleMongo a where a.show = :show and " +
                    "(a.displayDate is null or a.displayDate < :date1 or a.displayDate > :date2) " +
                    "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        List<ArticleMongo> results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        Assert.assertEquals("First article", results.get(0).getTitle());
        Assert.assertEquals("Second article", results.get(1).getTitle());
        Assert.assertEquals("Fourth article", results.get(2).getTitle());
        Assert.assertEquals("Fifth article", results.get(3).getTitle());

        query = em.createQuery(
              "select a from ArticleMongo a where a.show = :show and " +
                    "(a.displayDate is null or a.displayDate < :date1 or a.displayDate > :date2 and a.category is null) " +
                    "order by a.createDate asc");
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

        query = em.createQuery(
              "select a from ArticleMongo a where a.show = :show and " +
                    "(a.displayDate > :date2 and a.category is null or a.displayDate is null or a.displayDate < :date1) " +
                    "order by a.createDate asc");
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

        query = em.createQuery(
              "select a from ArticleMongo a where a.show = :show and " +
                    "(a.category is null and (a.displayDate > :date2 or a.displayDate is null or a.displayDate < :date1)) " +
                    "order by a.createDate asc");
        query.setParameter("show", true);
        query.setParameter("date1", date("2017-01-20 00:00"));
        query.setParameter("date2", date("2017-02-01 00:00"));
        results = query.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("Second article", results.get(0).getTitle());
        Assert.assertEquals("Fifth article", results.get(1).getTitle());

        query = em.createQuery(
              "select a from ArticleMongo a where a.show = :show and " +
                    "(a.createDate < :date and (a.displayDate is null or a.displayDate < :date)) " +
                    "and a.category = :category " +
                    "order by a.createDate asc");
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

    private void prepareArticle(String id, Date createDate, Date displayDate,
                                String title, String category, int priority, boolean show)
    {
        ArticleMongo item = new ArticleMongo();
        item.setArticleId(id);
        item.setCreateDate(createDate);
        item.setDisplayDate(displayDate);
        item.setTitle(title);
        item.setCategory(category);
        item.setPriority(priority);
        item.setShow(show);
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
