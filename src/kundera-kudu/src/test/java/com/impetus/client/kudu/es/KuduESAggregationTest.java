/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.kudu.es;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
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

import com.impetus.client.kudu.entities.Item;

import junit.framework.Assert;

/**
 * The Class KuduESAggregationTest.
 * 
 * @author devender.yadav
 */
public class KuduESAggregationTest
{

    /** The node. */
    private static Node node = null;

    /** The Constant KUDU_PU. */
    private static final String KUDU_PU = "esIndexerTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
        emf = Persistence.createEntityManagerFactory(KUDU_PU);
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
        init();
    }

    /**
     * Test aggregation.
     */
    @Test
    public void testAggregations()
    {
        testMinAggregation();
        testMaxAggregation();
        testSumAggregation();
        testAvgAggregation();
        testMinMaxSumAvgAggregation();
        testAggregationWithWhereClause();
        testLikeQuery();
    }

    /**
     * Test min aggregation.
     */
    private void testMinAggregation()
    {
        String queryString = "Select min(i.price) from Item i";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test max aggregation.
     */
    private void testMaxAggregation()
    {
        String queryString = "Select max(i.price) from Item i";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(400.0, resultList.get(0));
    }

    /**
     * Test sum aggregation.
     */
    private void testSumAggregation()
    {
        String queryString = "Select sum(i.quantity) from Item i";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test avg aggregation.
     */
    private void testAvgAggregation()
    {
        String queryString = "Select avg(i.quantity) from Item i";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(25.0, resultList.get(0));
    }

    /**
     * Test min max sum avg aggregation.
     */
    private void testMinMaxSumAvgAggregation()
    {
        String queryString = "Select min(i.price), max(i.price), sum(i.price), avg(i.price) from Item i";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
        Assert.assertEquals(1000.0, resultList.get(2));
        Assert.assertEquals(250.0, resultList.get(3));
    }

    /**
     * Test aggregation with where clause.
     */
    private void testAggregationWithWhereClause()
    {

        String queryString = "Select min(i.price) from Item i where i.quantity > 25";
        Query query = em.createNamedQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(300.0, resultList.get(0));
    }

    /**
     * Test like query.
     */
    private void testLikeQuery()
    {
        String queryString = "Select max(i.quantity) from Item i where i.name like 'item%'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    /**
     * Inits the.
     *
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void init() throws InterruptedException
    {

        Item item1 = new Item("1", "item1", 10, 100.0);
        Item item2 = new Item("2", "item2", 20, 200.0);
        Item item3 = new Item("3", "item3", 30, 300.0);
        Item item4 = new Item("4", "item4", 40, 400.0);

        em.persist(item1);
        em.persist(item2);
        em.persist(item3);
        em.persist(item4);

        waitThread();
    }

    /**
     * Delete records.
     *
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void deleteRecords() throws InterruptedException
    {
        em.remove(em.find(Item.class, "1"));
        em.remove(em.find(Item.class, "2"));
        em.remove(em.find(Item.class, "3"));
        em.remove(em.find(Item.class, "4"));
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
        deleteRecords();
        if (em != null)
        {
            em.close();
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

        if (emf != null)
        {
            emf.close();
        }

        if (node != null)
            node.close();
    }

    /**
     * Check if server running.
     *
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Wait thread.
     *
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }

}