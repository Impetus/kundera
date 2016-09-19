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
package com.impetus.client.es;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

/**
 * The Class CRUDTestWithRefreshedIndexes.
 * 
 * @author devender.yadav
 * 
 */
public class CRUDTestWithRefreshedIndexes
{

    /** The emf. */
    protected EntityManagerFactory emf;

    /** The em. */
    protected EntityManager em;

    /** The node. */
    private static Node node = null;

    /**
     * Sets the up before class.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        node = new NodeBuilder().settings(builder).node();
    }

    /**
     * Setup.
     */
    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("es-refresh-indexes-pu");
        em = emf.createEntityManager();
        /*
         * This property will force ES to refresh indexes on all the nodes after
         * insert, update and delete operations.
         * 
         */
        em.setProperty("es.refresh.indexes", true);
    }

    /**
     * Test crud.
     *
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testCRUD() throws InterruptedException
    {

        /*
         * Insert 100 book records
         */

        for (int i = 1; i <= 100; i++)
        {
            Book b = new Book();

            b.setBookId(1000 + i);
            b.setAuthor("author_" + i);
            b.setTitle("title_" + i);
            b.setNumPages(100 + i);

            em.persist(b);
        }

        Query q = em.createQuery("select count(b) from Book b");
        List bookList = q.getResultList();
        Assert.assertEquals(100L, bookList.get(0));

        /*
         * Update titles of all the records
         * 
         */
        for (int i = 1; i <= 100; i++)
        {
            Book book = em.find(Book.class, 1000 + i);
            Assert.assertEquals(100 + i, book.getNumPages());
            Assert.assertEquals("title_" + i, book.getTitle());
            Assert.assertEquals("author_" + i, book.getAuthor());
            book.setTitle("updated_title_" + i);
            em.merge(book);
        }

        for (int i = 1; i <= 100; i++)
        {
            Book book = em.find(Book.class, 1000 + i);
            Assert.assertEquals(100 + i, book.getNumPages());
            Assert.assertEquals("updated_title_" + i, book.getTitle());
            Assert.assertEquals("author_" + i, book.getAuthor());
        }

        /*
         * Delete all 100 records
         * 
         */
        q = em.createQuery("delete from Book b");
        int count = q.executeUpdate();

        // Assert.assertEquals(100, count);

        bookList = new ArrayList<>();
        q = em.createQuery("select count(b) from Book b");
        bookList = q.getResultList();
        Assert.assertEquals(0L, bookList.get(0));

        for (int i = 1; i <= 100; i++)
        {
            Book book = em.find(Book.class, 1000 + i);
            Assert.assertNull(book);
        }

    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        if (em != null)
        {
            em.close();
        }

        if (emf != null)
        {
            emf.close();
        }
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
        node.close();
    }

}
